package SparkTesting;

import java.io.Serializable;

import org.apache.spark.Partitioner;

public  class SimplePartitioner extends Partitioner  implements Serializable {

	private static final long serialVersionUID = 1L;
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
