package viettel.Spark.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

public class GenericLoadSaveFunction {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
										 .appName("Generic Load Save Function")
										 .getOrCreate();
		
		Dataset<Row> userDF = spark.read().load("file:///usr/local/spark/examples/src/main/resources/users.parquet");
		
		userDF.show();
		
		userDF.select("name", "favorite_color").show();
		
		
	}
}
