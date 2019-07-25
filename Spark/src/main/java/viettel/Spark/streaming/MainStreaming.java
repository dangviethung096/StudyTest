package viettel.Spark.streaming;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import viettel.Spark.Tables.KafkaTable;

public class MainStreaming {
	public static void main(String[] args) throws StreamingQueryException {
		SparkSession spark = SparkSession.builder()
										 .appName("Demo Streaming")
										 .config("spark.sql.streaming.schemaInference", "true")
										 .getOrCreate();
		
		Dataset<Row> df = spark.readStream().format("kafka")
											.option("kafka.bootstrap.servers", "10.55.123.60:9092")
											.option("subscribe", "kafka.vitalsign.ecg")
											.load();
											
		df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
		
		Dataset<KafkaTable> data = df.as(ExpressionEncoder.javaBean(KafkaTable.class));
		
		DataStreamWriter<KafkaTable> values =  data.writeStream().foreach(new ForeachWriter<KafkaTable>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			Configuration conf;
			FileSystem fs;
			FSDataOutputStream out;
			Path outputFile;
			@Override
			public void process(KafkaTable arg0) {
				System.out.println("Receive : " + arg0.getValue());
//				// TODO Auto-generated method stub
//				byte[] values = arg0.getValue().getBytes();
//				
//				try {
//					out.write(values);
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			}
			
			@Override
			public boolean open(long arg0, long arg1) {
				// TODO Auto-generated method stub
//				System.out.println("Open connection");
//				conf = new Configuration();
//				
//				outputFile = new Path("/home/hdfs/values.txt");
//				
//				try {
//					fs = FileSystem.get(conf);
//					
//					if(fs.exists(outputFile)) {
//						out = fs.append(outputFile);
//					} else {
//						out = fs.create(outputFile);
//					}
//					
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//					return false;
//				}
				
				return true;
			}
			
			@Override
			public void close(Throwable arg0) {
//				System.out.println("Close");
//				// TODO Auto-generated method stub
//				try {
//					out.close();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			}
		});
		
		
		StreamingQuery query = values.start();
		
		query.awaitTermination();
	}
}
