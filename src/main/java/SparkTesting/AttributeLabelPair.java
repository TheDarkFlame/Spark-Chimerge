package SparkTesting;

import java.io.Serializable;
import java.util.Map;

import com.google.common.collect.Maps;

@SuppressWarnings("serial")
public class AttributeLabelPair implements Serializable {
	
	private static Map<String, Double> classLabelMap = Maps.newHashMap();
	
	static {
		classLabelMap.put("Iris-virginica", 3.0);
		classLabelMap.put("Iris-versicolor", 2.0);
		classLabelMap.put("Iris-setosa", 1.0);
	}
	
	private String attributeName;
	private Double attributeValue;
	private Double classLabel;
	
	public AttributeLabelPair(String data) {
		String[] parts = data.split(",");
		this.attributeValue = Double.valueOf(parts[0]);
		this.classLabel = classLabelMap.get(parts[4]);
		this.attributeName = "Sepal Length";
	}

	public Double getAttributeValue() {
		return attributeValue;
	}

	public Double getClassLabel() {
		return classLabel;
	}

	@Override
	public String toString() {
		return attributeValue.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((attributeName == null) ? 0 : attributeName.hashCode());
		result = prime * result + ((attributeValue == null) ? 0 : attributeValue.hashCode());
		result = prime * result + ((classLabel == null) ? 0 : classLabel.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AttributeLabelPair other = (AttributeLabelPair) obj;
		if (attributeName == null) {
			if (other.attributeName != null)
				return false;
		} else if (!attributeName.equals(other.attributeName))
			return false;
		if (attributeValue == null) {
			if (other.attributeValue != null)
				return false;
		} else if (!attributeValue.equals(other.attributeValue))
			return false;
		if (classLabel == null) {
			if (other.classLabel != null)
				return false;
		} else if (!classLabel.equals(other.classLabel))
			return false;
		return true;
	}
	
}