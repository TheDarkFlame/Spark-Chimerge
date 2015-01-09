package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;

public class ChiSqaureTable implements Serializable{
	
	private static final long serialVersionUID = 1L;

	public static BigDecimal getChiSquareValue(int dof, double threshold) {
		
//		Table<Integer, BigDecimal, BigDecimal> table = HashBasedTable.create();
		
		//TODO: Change this with real static table
		return BigDecimal.valueOf(4.605);
	}

}
