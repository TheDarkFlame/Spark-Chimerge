package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import com.google.common.collect.Lists;

public class BlockMapPartition implements Serializable, PairFlatMapFunction<Iterator<Block>, BigDecimal, Block> {

	private static final long serialVersionUID = 1L;

	public Iterable<Tuple2<BigDecimal, Block>> call(Iterator<Block> t) throws Exception {
		List<Tuple2<BigDecimal, Block>> list = Lists.newArrayList();
		while (t.hasNext()) {
			Block b = t.next();
			list.add(new Tuple2<BigDecimal, Block>(b.getFingerPrint(), b));
		}
		return list;
	}
}
