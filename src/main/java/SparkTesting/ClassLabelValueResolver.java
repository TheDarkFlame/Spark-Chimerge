package SparkTesting;

import java.io.Serializable;
import java.util.Map;

import com.google.common.collect.Maps;

public class ClassLabelValueResolver implements Serializable {

	private static final long serialVersionUID = 1L;
	private Map<String, Double> map = Maps.newConcurrentMap();
	
	public ClassLabelValueResolver(String commaSeparatedDistinctValues) {
		final String[] labels = commaSeparatedDistinctValues.split(",");
		double i = 0;
		for(String classLabel: labels) {
			map.put(classLabel.trim().toLowerCase(), i++);
		}
	}
	
	public Double getClassLabelValue(String classLabel) {
		return map.get(classLabel.trim().toLowerCase());
	}
	
	public Integer getNumberOfClassLabels() {
		return this.map.size();
	}
}
