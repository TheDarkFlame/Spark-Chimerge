package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Comparator;

@SuppressWarnings("serial")
public class ChiSquareUnitSorter implements Comparator<ChisquareUnit>, Serializable {
	
	public int compare(ChisquareUnit o1, ChisquareUnit o2) {
		return BigDecimal.valueOf(o1.getChiSquareValue()).compareTo(BigDecimal.valueOf(o2.getChiSquareValue()));
	}
}