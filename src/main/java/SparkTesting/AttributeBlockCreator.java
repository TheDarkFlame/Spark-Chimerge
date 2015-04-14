package SparkTesting;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.function.Function;

import com.google.common.collect.Lists;

public class AttributeBlockCreator implements Function<Iterable<AttributeLabelPair>, Block>, Serializable {

	private static final long serialVersionUID = 1L;

	public Block call(Iterable<AttributeLabelPair> v1) throws Exception {
		List<AttributeLabelPair> records = Lists.newArrayList(v1);
		records.get(0).getAttributeValue();
		return new Block(records, records.get(0).getAttributeValue());
	}

}
