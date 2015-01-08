package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.collect.Lists;


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
@SuppressWarnings({ "serial" })
public class ChimergeDiscretizer implements Serializable {
	
	public static void main(String[] args) {
		Logger.getRootLogger().setLevel(Level.OFF);
	    JavaSparkContext jsc = setupSpark();
	    
	    //TODO: How to  distinct classLabels
	    ClassLabelValueResolver resolver = new ClassLabelValueResolver("Iris-setosa, Iris-virginica, Iris-versicolor");
	    
	    //TODO: How to getColumnLabels
	    String[] columns = {"SL","SW","PL","PW"};
	    
	    long startTime = System.currentTimeMillis(); // For time measurement.
	    
	    // Read the data from the file.
	    JavaRDD<String> stringRdd = jsc.textFile("./testData/Iris.txt", 3);
	    
	    JavaRDD<RawDataLine> rawData = stringRdd.map(new Function<String, RawDataLine>() {
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
		    JavaPairRDD<BigDecimal, AttributeLabelPair> mapToPair = data.mapToPair(new PairFunction<AttributeLabelPair, BigDecimal, AttributeLabelPair>() {
				public Tuple2<BigDecimal, AttributeLabelPair> call(AttributeLabelPair t) throws Exception {
					return new Tuple2<BigDecimal, AttributeLabelPair>(BigDecimal.valueOf(t.getAttributeValue()), t);
				}
			});
		    
		    // Group by key to pull all records with same value together.
		    JavaPairRDD<BigDecimal, Iterable<AttributeLabelPair>> groupByKey = mapToPair.groupByKey();
		    
		    // Now lets create a block which contains value and all its records which have that value. We need this for computing
		    // Chisquare.
		    JavaPairRDD<BigDecimal, Block> blocks = groupByKey.mapValues(new AttributeBlockCreator());
		    
		    BigDecimal min = BigDecimal.valueOf(Double.MIN_VALUE);
			BigDecimal threshold = ChiSqaureTable.getChiSquareValue(2, 0.001d);
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
			    		.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<BigDecimal, Block>>, Integer, Tuple2<BigDecimal, Block>>() {
					public Tuple2<Integer, Tuple2<BigDecimal, Block>> call(Tuple2<Integer, Tuple2<BigDecimal, Block>> t) throws Exception {
						return t;
					}
				})
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
			    	
			    	sourceRdd = sourceRdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Block>, BigDecimal, Block>() {
						public Iterable<Tuple2<BigDecimal, Block>> call(Iterator<Block> t) throws Exception {
							List<Tuple2<BigDecimal, Block>> list = Lists.newArrayList();
							while(t.hasNext()) {
								Block b = t.next();
								list.add(new Tuple2<BigDecimal, Block>(b.getFingerPrint(), b));
							}
							return list;
						}
					})
					.mapPartitionsWithIndex(new PartitionDataHandler(true), true)
					.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<BigDecimal,Block>>, Integer, Tuple2<BigDecimal, Block>>() {
		
						public Tuple2<Integer, Tuple2<BigDecimal, Block>> call(Tuple2<Integer, Tuple2<BigDecimal, Block>> t)
								throws Exception {
							return t;
						}
					})
					.partitionBy(new SimplePartitioner(sourceRdd.partitions().size()))
					.values()
					.map(new Function<Tuple2<BigDecimal,Block>, Block>() {
		
						public Block call(Tuple2<BigDecimal, Block> v1) throws Exception {
							return v1._2();
						}
					});
					
			    } while(i < sourceRdd.partitions().size());
			    // ******* end loop : Merging/reducing  *******
			    
			    // Here now all the Blocks with Chisquare = min have been merged.
			    // Lets compute ChiSquare until Chisquare is greater than the allowed threshold
			    blocks = sourceRdd.mapToPair(new PairFunction<Block, BigDecimal, Block>() {
	
					public Tuple2<BigDecimal, Block> call(Block t) throws Exception {
						return new Tuple2<BigDecimal, Block>(t.getFingerPrint(), t);
					}
				});
			    
		    } // end of big while (Threshold value)
	
			Utils.printBlockRanges(sourceRdd);
	    }
	    System.out.println("Time to run: " + (System.currentTimeMillis() - startTime));	    
	    jsc.stop();
	    
	}
	
	public static JavaSparkContext setupSpark() {
		SparkConf sparkConf = new SparkConf().setAppName("Chimerge");
		sparkConf.setMaster("local");
	    //sparkConf.setMaster("spark://BELC02MQ17MFD58.sea.corp.expecn.com:7077");
	    sparkConf.set("spark.executor.memory", "1g");
	    sparkConf.set("spark.driver.memory", "1g");
	    return new JavaSparkContext(sparkConf);
	}
}
