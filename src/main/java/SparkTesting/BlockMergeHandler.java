package SparkTesting;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.google.common.collect.Lists;

public class BlockMergeHandler implements Serializable, FlatMapFunction<Iterator<Block>, Block>{

	private static final long serialVersionUID = 1L;

	public Iterable<Block> call(Iterator<Block> t) throws Exception {
		List<Block> list = Lists.newArrayList();
		List<Block> mergedList = Lists.newArrayList();
		
		while (t.hasNext()) {
			list.add(t.next());
		}
		
		if(list.isEmpty()) {
			return mergedList;
		}

		Collections.sort(list, new Comparator<Block>() {
			public int compare(Block o1, Block o2) {
				return o1.getFingerPrint().compareTo(o2.getFingerPrint());
			}
		});
		
		Block current = list.get(0);
		int i = 1;
		while (i < list.size()) {
			Block next = list.get(i);
			if (current.contains(next)) {
				// Nothing.
			} else if (next.contains(current)) {
				current = next;
			} else if (current.overlaps(next)) {
				current = current.merge(next);
			} else {
				mergedList.add(current);
				current = next;
			}
			i++;
		}
		if (current != null) {
			mergedList.add(current);
		}
		return mergedList;
	}

}
