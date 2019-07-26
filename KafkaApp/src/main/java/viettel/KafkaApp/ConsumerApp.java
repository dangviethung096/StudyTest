package viettel.KafkaApp;

public class ConsumerApp {
	public static void main(String[] args) {
		KafkaJsonConsumer consumer = new KafkaJsonConsumer();
		
		try {
			consumer.receive();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
