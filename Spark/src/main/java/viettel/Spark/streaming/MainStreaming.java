package viettel.Spark.streaming;


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

import viettel.Spark.Cassandra.CassandraAPI;
import viettel.Spark.DataObjects.HeartRate;

import viettel.Spark.DataObjects.HeartRateRaw;
import viettel.Spark.DataObjects.KafkaObject;

public class MainStreaming {
	public static void main(String[] args) throws StreamingQueryException {
		SparkSession spark = SparkSession.builder()
										 .appName("Demo Streaming")
										 .config("spark.sql.streaming.schemaInference", "true")
										 .getOrCreate();
		
		Dataset<Row> df = spark.readStream().format("kafka")
											.option("kafka.bootstrap.servers", "10.55.123.60:9092")
											.option("subscribe", "kafka.vitalsign.heart-rate")
											.load();
											
		df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
		
		Dataset<KafkaObject> data = df.as(ExpressionEncoder.javaBean(KafkaObject.class));
		// Convert value to json file
		Dataset<HeartRate> heartRates = data.map(new MapFunction<KafkaObject, HeartRate>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public HeartRate call(KafkaObject value) throws Exception {
				// Convert from json
				ObjectMapper mapper = new ObjectMapper();
				HeartRateRaw heartRateRaw = mapper.readValue(value.getValue(), HeartRateRaw.class);
//				HeartRate heartRate = mapper.readValue(record.value(), HeartRate.class);
				
				HeartRate heartRate = new HeartRate();
				heartRate.setCurrent_heart_rate(heartRateRaw.getHeart_rate());
				
				return heartRate;
			}
			
		}, Encoders.bean(HeartRate.class));
		
		
		
//		ForeachWriter<KafkaObject> writeToCassandra = new ForeachWriter<KafkaObject>() {
//			
//			@Override
//			public void process(KafkaObject row) {
//				// TODO Auto-generated method stub
//				System.out.println("Receive value: heartRateAvg = " + row.getValue());
//				// Set value from kafka object
//				HeartRateAvg heartRateAvg = new HeartRateAvg();
//				heartRateAvg.setDay(new Timestamp(row.getTimestamp()));
//				heartRateAvg.setHeart_rate_avg(Double.parseDouble(row.getValue()));
//				heartRateAvg.setHeart_rate_max(Double.parseDouble(row.getValue()));
//				heartRateAvg.setHeart_rate_min(Double.parseDouble(row.getValue()));
//				heartRateAvg.setId(new UUID(row.getTimestamp(), row.getTimestamp()));
//				
//				CassandraAPI.getInstance().insertToDB(heartRateAvg);
//			}
//			
//			@Override
//			public boolean open(long arg0, long arg1) {
//				// TODO Auto-generated method stub
//				return false;
//			}
//			
//			@Override
//			public void close(Throwable arg0) {
//				// TODO Auto-generated method stub
//				
//			}
//		};
		
		ForeachWriter<HeartRate> writer = new ForeachWriter<HeartRate>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void close(Throwable arg0) {
				// TODO Auto-generated method stub
				CassandraAPI.close();
			}

			@Override
			public boolean open(long arg0, long arg1) {
				// TODO Auto-generated method stub
				try {
					CassandraAPI.connect();
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
				CassandraAPI.getInstance().insertToDB(heartRate);
			}
			
		};
		
		
		// Convert to 
		StreamingQuery query = heartRates.writeStream()
										 .foreach(writer)
										 .outputMode("append")
										 .format("console")
										 .start();
		
		query.awaitTermination();
	}
}
