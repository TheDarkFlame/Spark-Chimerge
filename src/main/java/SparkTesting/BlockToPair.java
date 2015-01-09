package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class BlockToPair implements Serializable, PairFunction<Block, BigDecimal, Block> {

	private static final long serialVersionUID = 1L;

	public Tuple2<BigDecimal, Block> call(Block t) throws Exception {
		return new Tuple2<BigDecimal, Block>(t.getFingerPrint(), t);
	}

}
