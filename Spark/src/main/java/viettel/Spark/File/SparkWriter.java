package viettel.Spark.File;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class SparkWriter {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Read localFile and write to hdfs").getOrCreate();
		
		Dataset<String> userDF = spark.read().format("text").load("file:///home/vttek/testFile.txt").as(Encoders.STRING());
		
		// Get file system
		Configuration conf = new Configuration();
//		conf.set("fs.hdfs.impl", org.apache.hadoop.)
//		conf.set("fs.file.impl", org.apache.hadoop.fs.Loca) 
		
		Path coreSite = new Path("file:/home/hadoop/hadoop-3.1.2/etc/hadoop/core-site.xml");
		Path hdfsSite = new Path("file:/home/hadoop/hadoop-3.1.2/etc/hadoop/hdfs-site.xml");
		
		conf.addResource(coreSite);
		conf.addResource(hdfsSite);
		
		
		
		try {
			FileSystem fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error when get FileSystem\n\n");
			e.printStackTrace();
			
		}
		
		
		
	}
}
