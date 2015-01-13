package SparkTesting;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class ChiSqaureTable implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private static final Table<Integer, BigDecimal, BigDecimal> table = HashBasedTable.create();
	
	static {
		load();
	}

	public static BigDecimal getChiSquareValue(int dof, double threshold) {
		// if the exact value is not found, decrease dof until you get a match
		
		if(!table.containsColumn(BigDecimal.valueOf(threshold))) {
			throw new IllegalArgumentException("Threshold does not exist.");
		}
		
		while(!table.containsRow(dof)) {
			dof--;
		}
		return table.get(dof, BigDecimal.valueOf(threshold));
	}
	
	private static void load() {
		File f = new File("./chisquareTable/ChiSquare.csv");
		List<String> lines = null;
		try {
			lines = FileUtils.readLines(f);
		} catch (IOException e) {
			throw new IllegalStateException();
		}
		
		int lineCount = 0;
		String[] header = null;
		
		for(String line: lines) {
			if( lineCount == 0 ) {
				header = line.split(",");
			} else {
				String[] s = line.split(",");
				Integer rowNumber = Integer.valueOf(s[0]);
				int index = 1;
				while(index < s.length) {
					BigDecimal value = new BigDecimal(s[index]);
					table.put(rowNumber, new BigDecimal(header[index++]), value);
				}
			}
			lineCount++;
		}
	}
}
