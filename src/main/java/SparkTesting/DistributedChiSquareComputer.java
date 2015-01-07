package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class DistributedChiSquareComputer implements Serializable, FlatMapFunction<Iterator<Tuple2<Integer,Tuple2<BigDecimal,Block>>>, ChisquareUnit> {

	public Iterable<ChisquareUnit> call(Iterator<Tuple2<Integer, Tuple2<BigDecimal, Block>>> t) throws Exception {
		List<Tuple2<BigDecimal, Block>> rawList = Lists.newArrayList();
		List<ChisquareUnit> returnList = Lists.newArrayList();
		
		while(t.hasNext()) {
			rawList.add(t.next()._2());
		}
		// Lets sort the data again. This is because when the data was added to the previous partition,
		// the sorted arrangement may have been lost. Since the partition fits into the worker's memory
		// we don't have to create a RDD to sort the partition data.
		Collections.sort(rawList, new Comparator<Tuple2<BigDecimal, Block>>() {

			public int compare(Tuple2<BigDecimal, Block> o1, Tuple2<BigDecimal, Block> o2) {
				return o1._1().compareTo(o2._1());
			}
		});

		for(int i = 0; i < rawList.size() - 1; i++) {
			ChisquareUnit unit = new ChisquareUnit(rawList.get(i)._2(), rawList.get(i + 1)._2());
			// Compute the ChiSquare.
			unit.computeChiSquare();
			returnList.add(unit);
		}
		return returnList;
	}

}
