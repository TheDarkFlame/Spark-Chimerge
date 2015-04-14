package SparkTesting;

import java.io.Serializable;

public class RawDataLine implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String[] parts;
	
	public RawDataLine(String data) {
		this.parts = data.split(",");
	}
	
	public String getClassLabel() {
		return parts[parts.length - 1];
	}
	
	public int numberOfAttributes() {
		return parts.length - 1; // The last one is a classLabel
	}


	public Double getDataAtIndex(int index) {
		if(index >= parts.length){
			throw new ArrayIndexOutOfBoundsException();
		}
		String part = parts[index];
		return Double.valueOf(part);
	}
}
