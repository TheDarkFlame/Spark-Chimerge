package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.concurrent.Callable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class ChimergeRunner implements Callable<JavaRDD<Block>>, Serializable {

	private static final long serialVersionUID = 1L;

	private JavaRDD<RawDataLine> rawData = null;
	
	private ClassLabelValueResolver resolver = null;
	
	private String columnName = null;
	
	private Integer position = null;
	
	private BigDecimal threshold = null;
	
	public ChimergeRunner(JavaRDD<RawDataLine> rawData, ClassLabelValueResolver resolver, String columnName,
			Integer position, BigDecimal threshold) {
		this.rawData = rawData;
		this.resolver = resolver;
		this.columnName = columnName;
		this.position = position;
		this.threshold = threshold;
	}

	public JavaRDD<Block> call() throws Exception {
		
	    //Step: Map raw line read from file to a AttributeLabelPair.
	    JavaRDD<AttributeLabelPair> data = rawData.map(new RawDataToAttributeConverter(position, resolver, columnName));
	    
	    // Create a JavaPairRDD with attribute value, record itself.
	    JavaPairRDD<BigDecimal, AttributeLabelPair> mapToPair = data.mapToPair(new RawDataToAttributePair());
	    
	    // Group by key to pull all records with same value together.
	    JavaPairRDD<BigDecimal, Iterable<AttributeLabelPair>> groupByKey = mapToPair.groupByKey();
	    
	    // Now lets create a block which contains value and all its records which have that value. We need this for computing
	    // Chisquare.
	    JavaPairRDD<BigDecimal, Block> blocks = groupByKey.mapValues(new AttributeBlockCreator());
	    
	    BigDecimal min = BigDecimal.valueOf(Double.MIN_VALUE);
		BigDecimal threshold = ChiSqaureTable.getChiSquareValue(resolver.getNumberOfClassLabels() - 1, this.threshold);
		JavaRDD<Block> sourceRdd = null;
		
		while(min.compareTo(threshold) < 0) {
		
		    // Lets sort the blocks by attribute value.
		    JavaPairRDD<BigDecimal, Block> sortedBlocksRdd = blocks.sortByKey(true);
		    
		    //Map Partitions With index
		    JavaRDD<Tuple2<Integer, Tuple2<BigDecimal, Block>>> mapPartitionsWithIndex = 
		    		sortedBlocksRdd.mapPartitionsWithIndex(new PartitionDataHandler(false), false);
		    
		    // We have not yet partitioned the data. We have just assigned each block to a partition number in the previous step.
		    // The below step creates the partitions based on the partition number we assigned previously.
		    JavaPairRDD<Integer, Tuple2<BigDecimal, Block>> mappedPartitions = mapPartitionsWithIndex
		    		.mapToPair(new TupleToPair())
			.partitionBy(
				new SimplePartitioner(sortedBlocksRdd.partitions().size())
			);
		    
		    // now create ChiSqUnit and compute chiSquare.
		    JavaRDD<ChisquareUnit> chiSquaredRdd = mappedPartitions.mapPartitions(new DistributedChiSquareComputer());
		    
		    // Compute the Global minimum of the Chisquare and then merge the Blocks with the minimum value. 
		    min = BigDecimal.valueOf(chiSquaredRdd.min(new ChiSquareUnitSorter()).getChiSquareValue());
		    
		    // Now lets return combined Block for all ChiSquareUnits whose value is equal to the minimum
		    // Otherwise, return the two blocks separate without combining them.
		    JavaRDD<Block> cm = chiSquaredRdd.mapPartitions(new BlockAggregator(min));
		    
		    // ******* begin loop : Merging/reducing  *******
		    // Here the idea is that, we always move the first element from the partition to the previous partition.
		    // i.e. move the first element from 1st partition to the oth partition. This way we give chance 
		    // for the elements to merge across partitions.
		    // the number of iterations  = partitions size. We need to give each partition a fair chance. 
		    sourceRdd = cm;
		    
		    int i = 0;
		    do {
		    	i++;
		    	sourceRdd = sourceRdd.mapPartitions(new BlockMergeHandler());
		    	
		    	sourceRdd = sourceRdd.mapPartitionsToPair(new BlockMapPartition())
				.mapPartitionsWithIndex(new PartitionDataHandler(true), true)
				.mapToPair(new TupleToPair())
				.partitionBy(new SimplePartitioner(sourceRdd.partitions().size()))
				.values()
				.map(new Function<Tuple2<BigDecimal,Block>, Block>() {
	
					private static final long serialVersionUID = 1L;

					public Block call(Tuple2<BigDecimal, Block> v1) throws Exception {
						return v1._2();
					}
				});
				
		    } while(i < sourceRdd.partitions().size());
		    // ******* end loop : Merging/reducing  *******
		    
		    // Here now all the Blocks with Chisquare = min have been merged.
		    // Lets compute ChiSquare until Chisquare is greater than the allowed threshold
		    blocks = sourceRdd.mapToPair(new BlockToPair());
		    
	    } // end of big while (Threshold value)
		
		return sourceRdd;
	}
}
