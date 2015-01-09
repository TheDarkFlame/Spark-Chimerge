package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TupleToPair implements Serializable, 
	PairFunction<Tuple2<Integer,Tuple2<BigDecimal, Block>>, Integer, Tuple2<BigDecimal, Block>> {
	
	private static final long serialVersionUID = 1L;

	public Tuple2<Integer, Tuple2<BigDecimal, Block>> call(Tuple2<Integer, Tuple2<BigDecimal, Block>> t) throws Exception {
		return t;
	}


}
