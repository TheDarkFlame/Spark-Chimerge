package SparkTesting;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class RawDataToAttributeConverter implements Serializable, Function<RawDataLine, AttributeLabelPair> {
	
	private ClassLabelValueResolver resolver;
	
	private int position;
	
	private String colName;
	
	public RawDataToAttributeConverter(int position, ClassLabelValueResolver resolver, String colName) {
		this.position = position;
		this.resolver = resolver;
		this.colName = colName;
	}
	
	public AttributeLabelPair call(RawDataLine v1) throws Exception {
		return new AttributeLabelPair(
					v1.getDataAtIndex(position), 
					resolver.getClassLabelValue(v1.getClassLabel()), 
					this.colName
				);
	}
}