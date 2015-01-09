package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class RawDataToAttributePair implements Serializable,
		PairFunction<AttributeLabelPair, BigDecimal, AttributeLabelPair> {

	private static final long serialVersionUID = 1L;

	public Tuple2<BigDecimal, AttributeLabelPair> call(AttributeLabelPair t) throws Exception {
		return new Tuple2<BigDecimal, AttributeLabelPair>(BigDecimal.valueOf(t.getAttributeValue()), t);
	}
}
