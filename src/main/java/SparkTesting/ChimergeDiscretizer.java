package SparkTesting;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.clearspring.analytics.util.Lists;


/**
 * @author Varsha Herle (vherle@uw.edu)
 * 
 * Algorithm per column(Attribute, Label Pair) 
 *  1. Unique attribute value
 *  2. Sort by attribute value
 *  3. partition with redundancy.
 *  4. combine adjacent blocks and compute ChiSquare
 *  5. Take global minimum.
 *  6. merge ChiSquareUnits
 *  7. Merge blocks in adjacent partitions until Blocks don't merge further. (While loop)
 *  8. Then back to Step 2:
 *
 */
public class ChimergeDiscretizer implements Serializable {
	
	private static final long serialVersionUID = 1L;

	private static Integer NUM_ROWS_PER_PARTITION = 100000; // 100,000. Default
	
	private static BigDecimal THRESHOLD = BigDecimal.valueOf(0.1d); //Default
	
	public static void main(String[] args) throws Exception {
		Logger.getRootLogger().setLevel(Level.OFF);
		Properties properties = Utils.loadProperties(args[0]);
		if(properties == null) {
			throw new IllegalStateException("Please provide properties file.");
		}
	    JavaSparkContext jsc = setupSpark(properties);
	    
	    ClassLabelValueResolver resolver = 
	    		new ClassLabelValueResolver(properties.getProperty(Property.APP_CLASS_LABEL_VALUES.getPropertyName()));
	    
	    String[] columns = properties.getProperty(Property.APP_COLUMN_NAMES.getPropertyName()).split(",");
	    
	    int dataSize = Integer.valueOf(properties.getProperty(Property.APP_APPROX_DATA_ROWS.getPropertyName()));
	    
	    if(properties.getProperty(Property.APP_ROWS_PER_PARTITION.getPropertyName()) != null) {
	    	NUM_ROWS_PER_PARTITION = Integer.valueOf(properties.getProperty(Property.APP_ROWS_PER_PARTITION.getPropertyName()));
	    }
	
	    if(properties.getProperty(Property.APP_CHISQUARE_THRESHOLD.getPropertyName()) != null) {
	    	THRESHOLD = new BigDecimal(properties.getProperty(Property.APP_CHISQUARE_THRESHOLD.getPropertyName()));
	    }
	    
	    int numOfPartitions = (dataSize / NUM_ROWS_PER_PARTITION) + 1;
	    
	    long startTime = System.currentTimeMillis(); // For time measurement.
	    
	    String dataFile = properties.getProperty(Property.APP_DATA_FILE.getPropertyName());
	    JavaRDD<String> stringRdd = jsc.textFile(dataFile, numOfPartitions);
	    
	    JavaRDD<RawDataLine> rawData = stringRdd.map(new Function<String, RawDataLine>() {
			private static final long serialVersionUID = 1L;

			public RawDataLine call(String v1) throws Exception {
				return new RawDataLine(v1);
			}
		});
	    
	    // Compute Chimerge for all attributes.
	    ExecutorService executorService  = Executors.newFixedThreadPool(10);
	    List<Future<JavaRDD<Block>>> futureList = Lists.newArrayList();
	    
	    try {
		    int numberOfAttributes = rawData.first().numberOfAttributes(); 
		    for(int j = 0; j < numberOfAttributes; j++) {
		    	futureList.add(executorService.submit(
		    			new ChimergeRunner(rawData, resolver, columns[j], j, THRESHOLD)));
		    }
		    
		    for(Future<JavaRDD<Block>> f: futureList) {
		    	Utils.printBlockRanges(f.get());
		    }
	    } finally {
	    	System.out.println("Time to run: " + (System.currentTimeMillis() - startTime));
	    	executorService.shutdown();
		    jsc.stop();
	    }
	}
	
	public static JavaSparkContext setupSpark(Properties properties) {
		
		SparkConf sparkConf = new SparkConf().setAppName("Spark-Chimerge");
		if(properties.getProperty(Property.CONF_MASTER_URL.getPropertyName()) != null) {
			sparkConf.setMaster(properties.getProperty(Property.CONF_MASTER_URL.getPropertyName()));
		}
		
		if(properties.getProperty(Property.CONF_EXECUTOR_MEMORY.getPropertyName()) != null) {
			sparkConf.set("spark.executor.memory", properties.getProperty(Property.CONF_EXECUTOR_MEMORY.getPropertyName()));
		}
		
		if(properties.getProperty(Property.CONF_DRIVER_MEMORY.getPropertyName()) != null) {
			sparkConf.set("spark.driver.memory", properties.getProperty(Property.CONF_DRIVER_MEMORY.getPropertyName()));
		}
	    return new JavaSparkContext(sparkConf);
	}
}
