package SparkTesting;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.function.Function;

import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class AttributeBlockCreator implements Function<Iterable<AttributeLabelPair>, Block>, Serializable {

	public Block call(Iterable<AttributeLabelPair> v1) throws Exception {
		List<AttributeLabelPair> records = Lists.newArrayList(v1);
		records.get(0).getAttributeValue();
		return new Block(records, records.get(0).getAttributeValue());
	}

}
