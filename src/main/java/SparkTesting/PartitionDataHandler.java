package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class PartitionDataHandler implements Function2<Integer, Iterator<Tuple2<BigDecimal, Block>>, Iterator<Tuple2<Integer, Tuple2<BigDecimal, Block>>>>, Serializable {
	
	private boolean moveFirstRecordToPreviousPartition = false;
	
	public PartitionDataHandler(boolean moveFirstRecordToPreviousPartition) {
		this.moveFirstRecordToPreviousPartition = moveFirstRecordToPreviousPartition;
	}

	public Iterator<Tuple2<Integer, Tuple2<BigDecimal, Block>>> call(Integer v1, Iterator<Tuple2<BigDecimal, Block>> v2)
			throws Exception {
		
		List<Tuple2<Integer, Tuple2<BigDecimal, Block>>> list = Lists.newArrayList();
		while(v2.hasNext()) {
			Tuple2<BigDecimal, Block> next = v2.next();
			list.add(new Tuple2<Integer, Tuple2<BigDecimal,Block>>(v1, next));
		}
		if(! list.isEmpty() && v1 > 0) {
			// This step is the one which takes the first element from this
			// partition and puts it in the previous partition. 
			// Hence maintaining the data continuity even with partitions.
			
			Tuple2<BigDecimal, Block> firstRecord = list.get(0)._2();
			list.add(new Tuple2<Integer, Tuple2<BigDecimal,Block>>(v1 - 1, firstRecord));
			
			if(moveFirstRecordToPreviousPartition) {
				while(list.remove(new Tuple2<Integer, Tuple2<BigDecimal,Block>>(v1, firstRecord)));
			}
		}
		return list.iterator();
	}

}
