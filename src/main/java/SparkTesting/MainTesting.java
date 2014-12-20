package SparkTesting;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.clearspring.analytics.util.Lists;

/**
 * 1. Algorithm to be fixed.  (uniqueValues, combine their classLevel occurance. Change matrix impl and compute Chimerge).
 * 2. Accessing consecutive elements
 * 3. Take out for loop within reduce. 
 *
 */
@SuppressWarnings("unchecked")
public class MainTesting implements Serializable {
	
	public static void main(String[] args) {
		Logger.getRootLogger().setLevel(Level.OFF);
	    SparkConf sparkConf = new SparkConf().setAppName("Local");
	    sparkConf.setMaster("local");
	    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
	    
	    long time = System.currentTimeMillis();
	    JavaRDD<String> stringRdd = jsc.textFile("./testData/IrisDuplicate.txt", 4);
	    
	    JavaRDD<IrisRecord> data = stringRdd.map(new Function<String, IrisRecord>() {

			public IrisRecord call(String v1) throws Exception {
				return new IrisRecord(v1);
			}
		});
	    
	    JavaPairRDD<Double, IrisRecord> mapToPair = data.mapToPair(new PairFunction<IrisRecord, Double, IrisRecord>() {
			
			public Tuple2<Double, IrisRecord> call(IrisRecord t) throws Exception {
				return new Tuple2(t.getSepalLength(), t);
			}
		});
	    
	    JavaPairRDD<Double, Iterable<IrisRecord>> groupByKey = mapToPair.groupByKey();
	    
	    JavaPairRDD<Double, Blockie> blocks = groupByKey.mapValues(new Function<Iterable<IrisRecord>, Blockie>() {

			public Blockie call(Iterable<IrisRecord> v1) throws Exception {
				List<IrisRecord> records = Lists.newArrayList(v1);
				records.get(0).getSepalLength();
				return new Blockie(records, records.get(0).getSepalLength());
			}
		});
	    
	    JavaPairRDD<Double, Blockie> sortedBlocksRdd = blocks.sortByKey(true);
	    
	    //Map Partitions With index
	    JavaRDD<Tuple2<Integer, Tuple2<Double, Blockie>>> mapPartitionsWithIndex = sortedBlocksRdd.
	    		mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Double, Blockie>>, Iterator<Tuple2<Integer, Tuple2<Double, Blockie>>>>() {

			public Iterator<Tuple2<Integer, Tuple2<Double, Blockie>>> call(Integer v1,
					Iterator<Tuple2<Double, Blockie>> v2) throws Exception {
				List<Tuple2<Integer, Tuple2<Double, Blockie>>> list = Lists.newArrayList();
				List<Tuple2<Double, Blockie>> tupleList = Lists.newArrayList();
				while(v2.hasNext()) {
					tupleList.add(v2.next());
				}
				
				for(Tuple2<Double, Blockie> tuple: tupleList) {
					list.add(new Tuple2<Integer, Tuple2<Double,Blockie>>(v1, tuple));
				}
				 
				if(v1 > 0) {
					//This should not be done for first partition
					list.add(new Tuple2<Integer, Tuple2<Double,Blockie>>(v1 - 1, tupleList.get(0)));
				}
				return list.iterator();
			}
		}, false);
	    
	    //Map to Pair
	    JavaPairRDD<Integer, Tuple2<Double, Blockie>> mapToPartition = mapPartitionsWithIndex.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Double, Blockie>>, Integer, Tuple2<Double, Blockie>>() {

			public Tuple2<Integer, Tuple2<Double, Blockie>> call(Tuple2<Integer, Tuple2<Double, Blockie>> t) throws Exception {
				return new Tuple2<Integer, Tuple2<Double,Blockie>>(t._1(), t._2());
			}
		});
	    
	    JavaPairRDD<Integer, Tuple2<Double, Blockie>> mappedPartitions = mapToPartition.partitionBy(new SimplePartitioner(sortedBlocksRdd.partitions().size()));
	    
	    // apply function to each partition to reduce
	    // apple chimerge to each partition.
	    
	    
	    // Till here blocks are unique and sorted. Part of Problem 1.
	    System.out.println("Stopping");
	    jsc.stop();
	    
	}
	
	private static class Blockie implements Serializable {
		private List<IrisRecord> records;
		
		private Double id;

		public Blockie(List<IrisRecord> records, Double id) {
			this.records = records;
			this.id = id;
		}

		public List<IrisRecord> getRecords() {
			return records;
		}

		public Double getId() {
			return id;
		}
	}
	
	private static class SimplePartitioner extends Partitioner {

		private int partitions;
		
		public SimplePartitioner(int num) {
			this.partitions = num;
		}
		
		@Override
		public int getPartition(Object arg0) {
			return (Integer) arg0;
		}

		@Override
		public int numPartitions() {
			return this.partitions;
		}
		
	}
}
