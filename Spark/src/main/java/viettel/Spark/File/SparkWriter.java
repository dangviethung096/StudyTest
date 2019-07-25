package viettel.Spark.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkWriter {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Read localFile and write to hdfs").getOrCreate();
		
		Dataset<String> userDF = spark.read().format("text").load("file:///home/vttek/testFile.txt").as(Encoders.STRING());
		
		userDF.write().format("text").save("hdfs://10.55.123.60:9000/home/hdfs/testFile.txt");
	}
}
