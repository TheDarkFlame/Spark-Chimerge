package SparkTesting;

import org.apache.spark.api.java.JavaRDD;

public final class Utils {
	
	private Utils() {}
	
	public static void printBlockRanges(JavaRDD<Block> bh) {
		for (Block b : bh.collect()) {
			System.out.println(b.getRange());
		}
	}

}
