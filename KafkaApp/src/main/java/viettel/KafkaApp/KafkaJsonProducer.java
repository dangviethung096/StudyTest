package viettel.KafkaApp;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import viettel.DataObjects.HeartRateRaw;

import org.apache.kafka.clients.producer.KafkaProducer;;

public class KafkaJsonProducer {
	Producer<String, String> producer;
	
	public KafkaJsonProducer() {
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "10.55.123.60:9092");
		 props.put("acks", "all");
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	
		 producer = new KafkaProducer<>(props);
		 
		     
		 
	}
	
	public void send() {
		 Random random = new Random();
		 try {
			 for (int i = 0; i < 100; i++) {
				 System.out.println("Send heart-rate " + Integer.toString(i));
				 // random heart rate
				 HeartRateRaw heartRate = new HeartRateRaw();
				 heartRate.setHeart_rate(100 * random.nextDouble());
				 
				 producer.send(new ProducerRecord<String, String>("kafka.vitalsign.heart-rate",Integer.toString(i), heartRate.toString()));
				 System.out.println("Sleep");
				 Thread.sleep(3000L);
			 }
		 } catch (InterruptedException ie) {
			 System.out.println("Error when sleep!");
		 }
		 
		 producer.close();
	}
}


