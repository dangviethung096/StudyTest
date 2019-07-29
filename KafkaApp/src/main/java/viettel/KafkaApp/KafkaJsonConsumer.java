package viettel.KafkaApp;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import viettel.DataObjects.HeartRateRaw;

public class KafkaJsonConsumer {
	
	KafkaConsumer<String, String> consumer;
	
	public KafkaJsonConsumer() {
		 Properties props = new Properties();
	     props.setProperty("bootstrap.servers", "10.55.123.60:9092");
	     props.setProperty("group.id", "check");
	     props.setProperty("enable.auto.commit", "true");
	     props.setProperty("auto.commit.interval.ms", "1000");
	     props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     
	     consumer = new KafkaConsumer<>(props);
	     consumer.subscribe(Arrays.asList("kafka.vitalsign.heart-rate"));
	}
	
	public void receive() throws JsonParseException, JsonMappingException, IOException {
		 ObjectMapper mapper = new ObjectMapper();
	     // Catch consumer
	     while (true) {
	    	 
    		 ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
	         for (ConsumerRecord<String, String> record : records) {
	        	 try {
	        		 HeartRateRaw heartRate = mapper.readValue(record.value(), HeartRateRaw.class);
	        		 System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), heartRate.toString());
	        	 }catch (Exception e) {
	        		 e.printStackTrace();
	        	 }
	         }
	         
	     }
	}
}
