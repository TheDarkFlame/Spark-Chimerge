package SparkTesting;

public enum Property {
	
	APP_COLUMN_NAMES("column.names"),
	APP_CLASS_LABEL_VALUES("class.label.unique.values"),
	APP_DATA_FILE("data.filename.with.path"),
	APP_APPROX_DATA_ROWS("approx.data.rows"),
	APP_ROWS_PER_PARTITION("rows.per.partition"),
	APP_CHISQUARE_THRESHOLD("chisquare.threshold"),
	
	CONF_MASTER_URL("spark.master.url"),
	CONF_EXECUTOR_MEMORY("executor.memory"),
	CONF_DRIVER_MEMORY("driver.memory");
	
	private String name;
	
	private Property(String name) {
		this.name = name;
	}
	
	public String getPropertyName(){
		return this.name;
	}

}
