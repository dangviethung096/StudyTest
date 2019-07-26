package viettel.KafkaApp;

/**
 * Hello world!
 *
 */
public class ProducerApp 
{
    public static void main( String[] args )
    {
        KafkaJsonProducer producer = new KafkaJsonProducer();
        
        producer.send();
    }
}
