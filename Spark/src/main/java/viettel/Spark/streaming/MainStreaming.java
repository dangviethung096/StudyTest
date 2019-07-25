package viettel.Spark.streaming;


import java.sql.Timestamp;
import java.util.UUID;


import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import viettel.Spark.Cassandra.CassandraAPI;
import viettel.Spark.DataObjects.HeartRateAvg;
import viettel.Spark.DataObjects.KafkaObject;

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
		
		Dataset<KafkaObject> data = df.as(ExpressionEncoder.javaBean(KafkaObject.class));
		// Convert value to json file
		
		
		
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
		
		
		// Convert to 
		StreamingQuery query = data.writeStream()
								   .outputMode("append")
								   .format("console")
								   .start();
		
		query.awaitTermination();
	}
}
