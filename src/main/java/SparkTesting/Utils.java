package SparkTesting;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

public final class Utils implements Serializable{
	
	private static final long serialVersionUID = 1L;

	private Utils() {}
	
	public static void printBlockRanges(JavaRDD<Block> bh) {
		boolean firstCut = true;
		Double low = null;
		List<Block> blocks = bh.collect();
		Collections.sort(blocks, new Comparator<Block>() {

			public int compare(Block o1, Block o2) {
				return o1.getFingerPrint().compareTo(o2.getFingerPrint());
			}
		});
		
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
