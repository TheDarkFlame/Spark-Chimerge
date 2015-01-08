package SparkTesting;

import java.io.Serializable;
import java.util.Map;

import com.google.common.collect.Maps;

@SuppressWarnings("serial")
public class ClassLabelValueResolver implements Serializable {

	private Map<String, Double> map = Maps.newConcurrentMap();
	
	public ClassLabelValueResolver(String commaSeparatedDistinctValues) {
		final String[] labels = commaSeparatedDistinctValues.split(",");
		double i = 0;
		for(String classLabel: labels) {
			map.put(classLabel.trim(), i++);
		}
	}
	
	public Double getClassLabelValue(String classLabel) {
		return map.get(classLabel);
	}
}
