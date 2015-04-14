package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.google.common.collect.Lists;

public class BlockAggregator implements Serializable, FlatMapFunction<Iterator<ChisquareUnit>, Block> {
	
	private static final long serialVersionUID = 1L;
	private BigDecimal minimum; 
	
	public BlockAggregator(BigDecimal min) {
		this.minimum = min;
	}

	public Iterable<Block> call(Iterator<ChisquareUnit> t) throws Exception {
		List<Block> blocks = Lists.newArrayList();
		while(t.hasNext()) {
			ChisquareUnit chUnit = t.next();
			if (BigDecimal.valueOf(chUnit.getChiSquareValue()).compareTo(this.minimum) == 0) {
				blocks.add(chUnit.getBlock1().merge(chUnit.getBlock2()));
			} else {
				blocks.add(chUnit.getBlock1());
				blocks.add(chUnit.getBlock2());
			}
		}
		return blocks;
	}

}
