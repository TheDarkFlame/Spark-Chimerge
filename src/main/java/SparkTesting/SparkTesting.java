package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@SuppressWarnings("serial")
public class SparkTesting implements Serializable {

	public static void main(String[] args) {
		
		Logger.getRootLogger().setLevel(Level.OFF);
	    SparkConf sparkConf = new SparkConf().setAppName("Local");
	    sparkConf.setMaster("local");
	    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
	    
	    long time = System.currentTimeMillis();
	    JavaRDD<String> stringRdd = jsc.textFile("./testData/Iris.txt");
	    
	    JavaRDD<IrisRecord> data = stringRdd.map(new Function<String, IrisRecord>() {

			public IrisRecord call(String v1) throws Exception {
				return new IrisRecord(v1);
			}
		});
	    
	    List<IrisRecord> sorted = data.takeOrdered((int)data.count(), new Comp());
	    
		Set<Block> blocks = Sets.newHashSet();
		long id = 0;
	    for(IrisRecord rec: sorted) {
	    	blocks.add(new Block(id++, rec));
	    }
	    
	    // Start of loop.
		BigDecimal min = BigDecimal.valueOf(Double.MIN_VALUE);
		BigDecimal threshold = BigDecimal.valueOf(4.605);
		
//		int index = 0;
//		while(index < 100){
//		index++;
		while(min.compareTo(threshold) < 0) {
			
			List<Block> sortedBlocks = jsc.parallelize(Lists.newArrayList(blocks)).takeOrdered(blocks.size(), new BlockSorter());
			List<ChisquareUnit> chiSqUnit = Lists.newArrayList();
			for (int i = 0; i < sortedBlocks.size() - 1; i++) {
				chiSqUnit.add(new ChisquareUnit(sortedBlocks.get(i), sortedBlocks.get(i + 1)));
			}
			
			@SuppressWarnings("serial")
			JavaRDD<ChisquareUnit> rdd = jsc.parallelize(chiSqUnit).map(new Function<ChisquareUnit, ChisquareUnit>() {

				public ChisquareUnit call(ChisquareUnit v1) throws Exception {
					v1.computeChiSquare();
					return v1;
				}
			});
			
			min = BigDecimal.valueOf(rdd.min(new Sorter()).getChiSquareValue());
			final Double minimum = min.doubleValue();
			
			@SuppressWarnings("serial")
			List<Block> mergedBlocks = rdd.map(new Function<ChisquareUnit, List<Block>>() {

				public List<Block> call(ChisquareUnit v1) throws Exception {
					if (BigDecimal.valueOf(v1.getChiSquareValue()).compareTo(BigDecimal.valueOf(minimum)) == 0) {
						return Collections.singletonList(v1.getBlock1().merge(v1.getBlock2()));
					}
					return Lists.newArrayList(v1.getBlock1(), v1.getBlock2());
				}
			}).reduce(new Function2<List<Block>, List<Block>, List<Block>>() {

				public List<Block> call(List<Block> v1, List<Block> v2) throws Exception {
					List<Block> set = Lists.newArrayList();
					set.addAll(v1);
					set.addAll(v2);
					return set;
				}
			});
			
			// Now we have Blocks [1-2], [2], [3], [3], [4], [4-5] [5] [6]
			
			// we want Blocks [1-2] [3], [4-5], [6]. Lets do it.
			
			@SuppressWarnings("serial")
			Set<Block> correctedBlocks = jsc.parallelize(mergedBlocks).map(new Function<Block, Set<Block>>() {

				public Set<Block> call(Block v1) throws Exception {
					return Sets.newHashSet(v1);
				}
			}).reduce(new Function2<Set<Block>, Set<Block>, Set<Block>>() {

				public Set<Block> call(Set<Block> v1, Set<Block> v2) throws Exception {
					if(v2.size() == 1) {
						return mergeSets(v1, Lists.newArrayList(v2).get(0));
					}else {
						return mergeSets(v2, Lists.newArrayList(v1).get(0));
					}
				}
				
				private Set<Block> mergeSets(Set<Block> set, Block block) {
					Set<Block> returnSet = Sets.newHashSet();
					boolean found = false;
					for(Block b: set){
						if(b.contains(block)){
							found = true;
							returnSet.add(b);
						} else if(block.contains(b)) {
							found = true;
							returnSet.add(block);
						} else if(block.doesOverlap(b)) {
							found = true;
							returnSet.add(block.merge(b));
						} else {
							returnSet.add(b);
						}
					}
					if(found == false) {
						returnSet.add(block);
					}
					// handle Overlap
					return returnSet;
				}
			});
			
			blocks = correctedBlocks;
			
			
		} // end of While
		
		printBlocks(blocks);
		System.out.println("Time " + (System.currentTimeMillis() - time));
		jsc.stop();
	}
	
	private static void printBlocks(Set<Block> blocks){
		List<Block> asd = Lists.newArrayList(blocks);
		Collections.sort(asd, new BlockSorter());
		for(Block b: asd) {
			System.out.println(b.getRange());
		}
	}
	
	private static class Comp implements Comparator<IrisRecord>, Serializable {

		public int compare(IrisRecord o1, IrisRecord o2) {
			return new BigDecimal(o1.getSepalLength()).compareTo(new BigDecimal(o2.getSepalLength()));
		}
	}
	
	private static class BlockSorter implements Comparator<Block>, Serializable {

		public int compare(Block o1, Block o2) {
			if(o1.getId() < o2.getId()) {
				return -1;
			} else if(o1.getId() > o2.getId()) {
				return 1;
			}
			return 0;
		}
	}

	private static class Sorter implements Comparator<ChisquareUnit>, Serializable {

		public int compare(ChisquareUnit o1, ChisquareUnit o2) {
			return BigDecimal.valueOf(o1.getChiSquareValue()).compareTo(BigDecimal.valueOf(o2.getChiSquareValue()));
		}
	}
}
