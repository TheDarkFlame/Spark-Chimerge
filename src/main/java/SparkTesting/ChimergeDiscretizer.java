package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;

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

	private static final Integer NUM_ROWS_PER_PARTITION = 100000; // 100,000
	
	public static void main(String[] args) {
		Logger.getRootLogger().setLevel(Level.OFF);
	    JavaSparkContext jsc = setupSpark(false);
	    
	    //TODO: properties
	    ClassLabelValueResolver resolver = new ClassLabelValueResolver("Iris-setosa, Iris-virginica, Iris-versicolor");
	    
	    //TODO: properties
	    String[] columns = {"SL","SW","PL","PW"};
	    
	    //TODO: properties.
	    int dataSize = 150;
	    
	    int numOfPartitions = (dataSize / NUM_ROWS_PER_PARTITION) + 1;
	    
	    long startTime = System.currentTimeMillis(); // For time measurement.
	    
	    //TODO: properties - filename or argument
	    JavaRDD<String> stringRdd = jsc.textFile("./testData/Iris.txt", numOfPartitions);
	    
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
			BigDecimal threshold = ChiSqaureTable.getChiSquareValue(resolver.getNumberOfClassLabels() - 1, 0.1d);
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
	
	public static JavaSparkContext setupSpark(boolean local) {
		
		SparkConf sparkConf = new SparkConf().setAppName("Chimerge");
		if(local) {
			sparkConf.setMaster("local[4]");
		} else {
			//String[] jars = {"/Users/rmysoreradhakrishna/git-workspace/Spark-Chimerge/build/libs/Spark-Chimerge-1.0.jar"};
		    //sparkConf.setMaster("spark://BELC02MQ17MFD58.sea.corp.expecn.com:7077");
			//sparkConf.setSparkHome("/Users/rmysoreradhakrishna/Downloads/spark-1.1.0-bin-hadoop1/");
			//sparkConf.setJars(jars);
		}
	    sparkConf.set("spark.executor.memory", "1g");
	    sparkConf.set("spark.driver.memory", "1g");
	    return new JavaSparkContext(sparkConf);
	}
}
