package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;

@SuppressWarnings("serial")
public class ChiSqaureTable implements Serializable{
	
	public static BigDecimal getChiSquareValue(int dof, double threshold) {
		
		//TODO: Change this with real static table
		return BigDecimal.valueOf(4.605);
	}

}
