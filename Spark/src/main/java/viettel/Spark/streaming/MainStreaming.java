package viettel.Spark.streaming;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.fasterxml.jackson.databind.ObjectMapper;

import viettel.DataObjects.HeartRate;
import viettel.DataObjects.HeartRateRaw;
import viettel.DataObjects.KafkaObject;
import viettel.Spark.Cassandra.CassandraAPI;

public class MainStreaming {
	public static void main(String[] args) throws StreamingQueryException {
		SparkSession spark = SparkSession.builder()
										 .appName("Demo Streaming")
										 .config("spark.sql.streaming.schemaInference", "true")
										 .getOrCreate();
		
		Dataset<Row> df = spark.readStream().format("kafka")
											.option("kafka.bootstrap.servers", "10.55.123.60:9092")
											.option("subscribe", "kafka.vitalsign.heart-rate")
											.load()
											.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", 
														"CAST(topic AS STRING)", "CAST(partition AS INT)", 
														"CAST(offset AS LONG)","CAST(timestamp AS LONG)",
														"CAST(timestampType AS INT)");
		
		
		// Convert to kafka project
		Dataset<KafkaObject> data = df.as(ExpressionEncoder.javaBean(KafkaObject.class));
		
		// Convert value from json to HeartRate class
		Dataset<HeartRate> heartRates = data.map(new MapFunction<KafkaObject, HeartRate>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public HeartRate call(KafkaObject value) throws Exception {
				// Convert from json
				HeartRate heartRate  = new HeartRate();
				try {
					ObjectMapper mapper = new ObjectMapper();
					HeartRateRaw heartRateRaw = mapper.readValue(value.getValue(), HeartRateRaw.class);
					// Insert heart_rate
					heartRate.setId(UUID.randomUUID().toString());
					heartRate.setCurrentHeartRate(heartRateRaw.getHeart_rate());
					heartRate.setTime(new Timestamp(value.getTimestamp()));
					
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				
				heartRate.setTime(new Timestamp(System.currentTimeMillis() ));
				
				return heartRate;
				
			}
			
		}, Encoders.bean(HeartRate.class));
		
	
//		StreamingQuery writeToCassandra = MainStreaming.writeToCassandra(heartRates);
		StreamingQuery writeToHDFS = MainStreaming.writeToHDFS(heartRates);
		
		// Wait to termination
//		writeToCassandra.awaitTermination();
		writeToHDFS.awaitTermination();
	}
	
	public static StreamingQuery writeToHDFS(Dataset<HeartRate> heartRates) {
		// Define foreach function for write to database
		ForeachWriter<HeartRate> toHDFS = new ForeachWriter<HeartRate>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			
			private String outputFileStr = "/vht/medical/vitalsign/heart_rate.txt";
			FSDataOutputStream out;
			private final int BUFFER_SIZE = 1024;
			
			@Override
			public void close(Throwable arg0) {
				// TODO Auto-generated method stub
				try {
					if(out != null) {
						out.close();
						out = null;
					}
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			@Override
			public boolean open(long arg0, long arg1) {
				// TODO Auto-generated method stub
				try {
					// Get file system
					Configuration conf = new Configuration();
					FileSystem fs = FileSystem.get(conf);
					Path outputFile = new Path(outputFileStr);
		
					// Check file and get output stream
					if(fs.exists(outputFile)) {
						out = fs.append(outputFile);
					} else {
						out = fs.create(outputFile);
					}
				
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}
				
				return true;
			}

			@Override
			public void process(HeartRate heartRate) {
				// TODO Auto-generated method stub
				
				InputStream in = new ByteArrayInputStream(heartRate.toString().getBytes()); 
				byte[] buffer = new byte[BUFFER_SIZE];
				
				try {
					while(in.read(buffer) > 0) {
						out.write(buffer);
					}
					
					buffer = "\n".getBytes();
					out.write(buffer);
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		};
		
		// Write stream
		return heartRates.writeStream()
						 .foreach(toHDFS)
						 .start();
	}
	
	public static StreamingQuery writeToCassandra(Dataset<HeartRate> heartRates) {
		// Define foreach function for write to database
		ForeachWriter<HeartRate> writer = new ForeachWriter<HeartRate>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void close(Throwable arg0) {
				// TODO Auto-generated method stub
				CassandraAPI.getInstance().close();
			}

			@Override
			public boolean open(long arg0, long arg1) {
				// TODO Auto-generated method stub
				try {
					CassandraAPI.getInstance().connect();
				} catch (Exception ex) {
					ex.printStackTrace();
					System.err.println("Cassandra ERROR: cannot connect to db");
					return false;
				}
				
				return true;
			}

			@Override
			public void process(HeartRate heartRate) {
				// TODO Auto-generated method stub
				try {
					CassandraAPI.getInstance().insertToDB(heartRate);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		};
	
		// Return value
		return heartRates.writeStream().foreach(writer).start();
	}
}
