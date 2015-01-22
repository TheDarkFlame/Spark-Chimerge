package SparkTesting;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;


/**
 * @author Varsha Herle (vherle@uw.edu)
 * 
 * Algorithm per column(Attribute, Label Pair) 
 *  1. Unique attribute value
 *  2. Sort by attribute value
 *  3. partition with redundancy.
 *  4. combine adjacent blocks and compute ChiSquare
 *  5. Take global minimum.
 *  6. merge ChiSquareUnits
 *  7. Merge blocks in adjacent partitions until Blocks don't merge further. (While loop)
 *  8. Then back to Step 2:
 *
 */
public class ChimergeDiscretizer implements Serializable {
	
	private static final long serialVersionUID = 1L;

	private static Integer NUM_ROWS_PER_PARTITION = 100000; // 100,000. Default
	
	private static BigDecimal THRESHOLD = BigDecimal.valueOf(0.1d); //Default
	
	public static void main(String[] args) throws IOException {
		Logger.getRootLogger().setLevel(Level.OFF);
		Properties properties = Utils.loadProperties(args[0]);
		if(properties == null) {
			throw new IllegalStateException("Please provide properties file.");
		}
	    JavaSparkContext jsc = setupSpark(properties);
	    
	    ClassLabelValueResolver resolver = 
	    		new ClassLabelValueResolver(properties.getProperty(Property.APP_CLASS_LABEL_VALUES.getPropertyName()));
	    
	    String[] columns = properties.getProperty(Property.APP_COLUMN_NAMES.getPropertyName()).split(",");
	    
	    int dataSize = Integer.valueOf(properties.getProperty(Property.APP_APPROX_DATA_ROWS.getPropertyName()));
	    
	    if(properties.getProperty(Property.APP_ROWS_PER_PARTITION.getPropertyName()) != null) {
	    	NUM_ROWS_PER_PARTITION = Integer.valueOf(properties.getProperty(Property.APP_ROWS_PER_PARTITION.getPropertyName()));
	    }
	
	    if(properties.getProperty(Property.APP_CHISQUARE_THRESHOLD.getPropertyName()) != null) {
	    	THRESHOLD = new BigDecimal(properties.getProperty(Property.APP_CHISQUARE_THRESHOLD.getPropertyName()));
	    }
	    
	    int numOfPartitions = (dataSize / NUM_ROWS_PER_PARTITION) + 1;
	    
	    long startTime = System.currentTimeMillis(); // For time measurement.
	    
	    String dataFile = properties.getProperty(Property.APP_DATA_FILE.getPropertyName());
	    JavaRDD<String> stringRdd = jsc.textFile(dataFile, numOfPartitions);
	    
	    JavaRDD<RawDataLine> rawData = stringRdd.map(new Function<String, RawDataLine>() {
			private static final long serialVersionUID = 1L;

			public RawDataLine call(String v1) throws Exception {
				return new RawDataLine(v1);
			}
		});
	    
	    // Compute Chimerge for all attributes.

	    //TODO: make this parallel. Tricky
	    int numberOfAttributes = rawData.first().numberOfAttributes(); 
	    for(int j = 0; j < numberOfAttributes; j++) {
	    	
		    //Step: Map raw line read from file to a AttributeLabelPair.
		    JavaRDD<AttributeLabelPair> data = rawData.map(new RawDataToAttributeConverter(j, resolver, columns[j]));
		    
		    // Create a JavaPairRDD with attribute value, record itself.
		    JavaPairRDD<BigDecimal, AttributeLabelPair> mapToPair = data.mapToPair(new RawDataToAttributePair());
		    
		    // Group by key to pull all records with same value together.
		    JavaPairRDD<BigDecimal, Iterable<AttributeLabelPair>> groupByKey = mapToPair.groupByKey();
		    
		    // Now lets create a block which contains value and all its records which have that value. We need this for computing
		    // Chisquare.
		    JavaPairRDD<BigDecimal, Block> blocks = groupByKey.mapValues(new AttributeBlockCreator());
		    
		    BigDecimal min = BigDecimal.valueOf(Double.MIN_VALUE);
			BigDecimal threshold = ChiSqaureTable.getChiSquareValue(resolver.getNumberOfClassLabels() - 1, THRESHOLD);
			JavaRDD<Block> sourceRdd = null;
			
			while(min.compareTo(threshold) < 0) {
			
			    // Lets sort the blocks by attribute value.
			    JavaPairRDD<BigDecimal, Block> sortedBlocksRdd = blocks.sortByKey(true);
			    
			    //Map Partitions With index
			    JavaRDD<Tuple2<Integer, Tuple2<BigDecimal, Block>>> mapPartitionsWithIndex = 
			    		sortedBlocksRdd.mapPartitionsWithIndex(new PartitionDataHandler(false), false);
			    
			    // We have not yet partitioned the data. We have just assigned each block to a partition number in the previous step.
			    // The below step creates the partitions based on the partition number we assigned previously.
			    JavaPairRDD<Integer, Tuple2<BigDecimal, Block>> mappedPartitions = mapPartitionsWithIndex
			    		.mapToPair(new TupleToPair())
				.partitionBy(
					new SimplePartitioner(sortedBlocksRdd.partitions().size())
				);
			    
			    // now create ChiSqUnit and compute chiSquare.
			    JavaRDD<ChisquareUnit> chiSquaredRdd = mappedPartitions.mapPartitions(new DistributedChiSquareComputer());
			    
			    // Compute the Global minimum of the Chisquare and then merge the Blocks with the minimum value. 
			    min = BigDecimal.valueOf(chiSquaredRdd.min(new ChiSquareUnitSorter()).getChiSquareValue());
			    
			    // Now lets return combined Block for all ChiSquareUnits whose value is equal to the minimum
			    // Otherwise, return the two blocks separate without combining them.
			    JavaRDD<Block> cm = chiSquaredRdd.mapPartitions(new BlockAggregator(min));
			    
			    // ******* begin loop : Merging/reducing  *******
			    // Here the idea is that, we always move the first element from the partition to the previous partition.
			    // i.e. move the first element from 1st partition to the oth partition. This way we give chance 
			    // for the elements to merge across partitions.
			    // the number of iterations  = partitions size. We need to give each partition a fair chance. 
			    sourceRdd = cm;
			    
			    int i = 0;
			    do {
			    	i++;
			    	sourceRdd = sourceRdd.mapPartitions(new BlockMergeHandler());
			    	
			    	sourceRdd = sourceRdd.mapPartitionsToPair(new BlockMapPartition())
					.mapPartitionsWithIndex(new PartitionDataHandler(true), true)
					.mapToPair(new TupleToPair())
					.partitionBy(new SimplePartitioner(sourceRdd.partitions().size()))
					.values()
					.map(new Function<Tuple2<BigDecimal,Block>, Block>() {
		
						private static final long serialVersionUID = 1L;

						public Block call(Tuple2<BigDecimal, Block> v1) throws Exception {
							return v1._2();
						}
					});
					
			    } while(i < sourceRdd.partitions().size());
			    // ******* end loop : Merging/reducing  *******
			    
			    // Here now all the Blocks with Chisquare = min have been merged.
			    // Lets compute ChiSquare until Chisquare is greater than the allowed threshold
			    blocks = sourceRdd.mapToPair(new BlockToPair());
			    
		    } // end of big while (Threshold value)
	
			Utils.printBlockRanges(sourceRdd);
	    }
	    System.out.println("Time to run: " + (System.currentTimeMillis() - startTime));	    
	    jsc.stop();
	    
	}
	
	public static JavaSparkContext setupSpark(Properties properties) {
		
		SparkConf sparkConf = new SparkConf().setAppName("Spark-Chimerge");
		if(properties.getProperty(Property.CONF_MASTER_URL.getPropertyName()) != null) {
			sparkConf.setMaster(properties.getProperty(Property.CONF_MASTER_URL.getPropertyName()));
		}
		
		if(properties.getProperty(Property.CONF_EXECUTOR_MEMORY.getPropertyName()) != null) {
			sparkConf.set("spark.executor.memory", properties.getProperty(Property.CONF_EXECUTOR_MEMORY.getPropertyName()));
		}
		
		if(properties.getProperty(Property.CONF_DRIVER_MEMORY.getPropertyName()) != null) {
			sparkConf.set("spark.driver.memory", properties.getProperty(Property.CONF_DRIVER_MEMORY.getPropertyName()));
		}
	    return new JavaSparkContext(sparkConf);
	}
}
