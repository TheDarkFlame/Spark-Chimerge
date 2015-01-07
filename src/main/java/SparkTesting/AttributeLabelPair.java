package SparkTesting;

import java.io.Serializable;
import java.util.Map;

import com.google.common.collect.Maps;

public class AttributeLabelPair implements Serializable {
	
	private static Map<String, Double> irisMap = Maps.newHashMap();
	
	private static final long serialVersionUID = 1L;
	
	static {
		irisMap.put("Iris-virginica", 3.0);
		irisMap.put("Iris-versicolor", 2.0);
		irisMap.put("Iris-setosa", 1.0);
	}
	
	private Double petalLength;
	private Double petalWidth;
	private Double sepalLength;
	private Double sepalWidth;
	private Double species;
	
	public AttributeLabelPair(String data) {
		String[] parts = data.split(",");
		this.petalLength = Double.valueOf(parts[2]);
		this.petalWidth = Double.valueOf(parts[3]);
		this.sepalLength = Double.valueOf(parts[0]);
		this.sepalWidth = Double.valueOf(parts[1]);
		this.species = irisMap.get(parts[4]);
	}

	public Double getPetalLength() {
		return petalLength;
	}

	public Double getPetalWidth() {
		return petalWidth;
	}

	public Double getSepalLength() {
		return sepalLength;
	}

	public Double getSepalWidth() {
		return sepalWidth;
	}

	public Double getSpecies() {
		return species;
	}

	@Override
	public String toString() {
		return sepalLength.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sepalLength == null) ? 0 : sepalLength.hashCode());
		result = prime * result + ((species == null) ? 0 : species.hashCode());
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
		if (sepalLength == null) {
			if (other.sepalLength != null)
				return false;
		} else if (!sepalLength.equals(other.sepalLength))
			return false;
		if (species == null) {
			if (other.species != null)
				return false;
		} else if (!species.equals(other.species))
			return false;
		return true;
	}
}