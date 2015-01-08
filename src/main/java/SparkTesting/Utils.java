package SparkTesting;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;

public final class Utils {
	
	private Utils() {}
	
	public static void printBlockRanges(JavaRDD<Block> bh) {
		boolean firstCut = true;
		Double low = null;
		List<Block> blocks = bh.collect();
		System.out.println("Attribute: " + blocks.get(0).getAllRecords().get(0).getAttributeName());
		for (Block b : blocks) {
			if(firstCut) {
				System.out.println(b.getMinRange() + "  to " + b.getMaxRange());
				firstCut = false;
			} else {
				System.out.println(low + " to " + b.getMaxRange());
			}
			low = b.getMaxRange();
		}
	}
}
