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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.collect.Lists;


/**
 * Algorithm per column(Attribute, Label Pair) 
 *  1. Unique attribute value
 *  2. Sort by attribute value
 *  3. partition with redundancy.
 *  4. combine adjacent blocks and compute ChiSquare
 *  5. Take global minimum.
 *  6. merge ChiSquareUnits
 *  7. Merge adjacent blocks(by partitioning) until they don't further merge. (While loop)
 *  8. Then back to Step 2:
 *
 */
@SuppressWarnings({ "serial" })
public class ChimergeDiscretizer implements Serializable {
	
	public static void main(String[] args) {
		Logger.getRootLogger().setLevel(Level.OFF);
	    JavaSparkContext jsc = setupSpark();
	    
	    long startTime = System.currentTimeMillis(); // For time measurement.
	    
	    // Read the data from the file.
	    JavaRDD<String> stringRdd = jsc.textFile("./testData/Iris.txt", 3);
	    
	    //Step: Map raw line read from file to a AttributeLabelPair.
	    JavaRDD<AttributeLabelPair> data = stringRdd.map(new Function<String, AttributeLabelPair>() {
			public AttributeLabelPair call(String v1) throws Exception {
				return new AttributeLabelPair(v1);
			}
		});
	    
	    // Create a JavaPairRDD with attribute value, record itself.
	    JavaPairRDD<Double, AttributeLabelPair> mapToPair = data.mapToPair(new PairFunction<AttributeLabelPair, Double, AttributeLabelPair>() {
			public Tuple2<Double, AttributeLabelPair> call(AttributeLabelPair t) throws Exception {
				return new Tuple2<Double, AttributeLabelPair>(t.getAttributeValue(), t);
			}
		});
	    
	    //Group by key to pull all records with same value together.
	    JavaPairRDD<Double, Iterable<AttributeLabelPair>> groupByKey = mapToPair.groupByKey();
	    
	    //Now lets create a block which contains value and all its records which have that value. We need this for computing
	    // Chisquare.
	    JavaPairRDD<Double, Block> blocks = groupByKey.mapValues(new AttributeBlockCreator());
	    
	    BigDecimal min = BigDecimal.valueOf(Double.MIN_VALUE);
		BigDecimal threshold = ChiSqaureTable.getChiSquareValue(2, 0.001d);
		JavaRDD<Block> sourceRdd = null;
		while(min.compareTo(threshold) < 0) {
		
		    // Lets sort the blocks by attribute value.
		    JavaPairRDD<Double, Block> sortedBlocksRdd = blocks.sortByKey(true);
		    
		    //Map Partitions With index
		    JavaRDD<Tuple2<Integer, Tuple2<Double, Block>>> mapPartitionsWithIndex = 
		    		sortedBlocksRdd.mapPartitionsWithIndex(new PartitionDataHandler(), false);
		    
		    // We have not yet partitioned the data. We have just assigned each block to a partition number in the previous step.
		    // The below step creates the partitions based on the partition number we assigned previously.
		    JavaPairRDD<Integer, Tuple2<Double, Block>> mappedPartitions = mapPartitionsWithIndex
		    		.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Double, Block>>, Integer, Tuple2<Double, Block>>() {
				public Tuple2<Integer, Tuple2<Double, Block>> call(Tuple2<Integer, Tuple2<Double, Block>> t) throws Exception {
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
		    final Double minimum = min.doubleValue();
		    
		    JavaRDD<Block> cm = chiSquaredRdd.mapPartitions(new FlatMapFunction<Iterator<ChisquareUnit>, Block>() {
	
				public Iterable<Block> call(Iterator<ChisquareUnit> t) throws Exception {
					List<Block> blocks = Lists.newArrayList();
					while(t.hasNext()) {
						ChisquareUnit chUnit = t.next();
						if (BigDecimal.valueOf(chUnit.getChiSquareValue()).compareTo(BigDecimal.valueOf(minimum)) == 0) {
							blocks.add(chUnit.getBlock1().merge(chUnit.getBlock2()));
						} else {
							blocks.add(chUnit.getBlock1());
							blocks.add(chUnit.getBlock2());
						}
					}
					return blocks;
				}
			});
		    
		    // ******* begin loop : Combine *******
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
				.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<BigDecimal,Block>>, Iterator<Tuple2<Integer, Tuple2<BigDecimal, Block>>>>() {
	
					public Iterator<Tuple2<Integer, Tuple2<BigDecimal, Block>>> call(Integer v1,
							Iterator<Tuple2<BigDecimal, Block>> v2) throws Exception {
						
						List<Tuple2<Integer, Tuple2<BigDecimal, Block>>> list = Lists.newArrayList();
						while(v2.hasNext()) {
							Tuple2<BigDecimal,Block> next = v2.next();
							list.add(new Tuple2<Integer, Tuple2<BigDecimal,Block>>(v1, next));
						}
						if(! list.isEmpty() && v1 > 0) {
							// This step is the one which takes the first element from this
							// partition and puts it in the previous partition. 
							// Hence maintaining the data continuity even with partitions.
							Tuple2<BigDecimal, Block> firstRecord = list.get(0)._2();
							list.add(new Tuple2<Integer, Tuple2<BigDecimal,Block>>(v1 - 1, firstRecord));
							while(list.remove(new Tuple2<Integer, Tuple2<BigDecimal,Block>>(v1, firstRecord)));
						}
						return list.iterator();
					}
				}, true)
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
		    
		    blocks = sourceRdd.mapToPair(new PairFunction<Block, Double, Block>() {

				public Tuple2<Double, Block> call(Block t) throws Exception {
					return new Tuple2<Double, Block>(t.getFingerPrint().doubleValue(), t);
				}
			});
		    
		    
	    } // end of big while (Threshold value)

		printBlockRanges(sourceRdd);
	    System.out.println("Time to run: " + (System.currentTimeMillis() - startTime));	    
	    jsc.stop();
	    
	}
	
	public static void printBlockRanges(JavaRDD<Block> bh) {
		for (Block b : bh.collect()) {
			System.out.println(b.getRange());
		}
	}
	
	public static JavaSparkContext setupSpark() {
		SparkConf sparkConf = new SparkConf().setAppName("Local");
	    sparkConf.setMaster("local");
	    sparkConf.set("spark.executor.memory", "1g");
	    sparkConf.set("spark.driver.memory", "1g");
	    return new JavaSparkContext(sparkConf);
	}
}
