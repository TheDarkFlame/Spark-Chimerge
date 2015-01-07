package SparkTesting;

import java.io.Serializable;

import org.apache.spark.Partitioner;

@SuppressWarnings("serial")
public  class SimplePartitioner extends Partitioner  implements Serializable {

	private int partitions;
	
	public SimplePartitioner(int num) {
		this.partitions = num;
	}
	
	@Override
	public int getPartition(Object arg0) {
		return (Integer) arg0;
	}

	@Override
	public int numPartitions() {
		return this.partitions;
	}
}
