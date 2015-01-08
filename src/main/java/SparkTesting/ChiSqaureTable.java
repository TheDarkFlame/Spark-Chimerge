package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

@SuppressWarnings("serial")
public class ChiSqaureTable implements Serializable{
	
	public static BigDecimal getChiSquareValue(int dof, double threshold) {
		
//		Table<Integer, BigDecimal, BigDecimal> table = HashBasedTable.create();
		
		//TODO: Change this with real static table
		return BigDecimal.valueOf(4.605);
	}

}
