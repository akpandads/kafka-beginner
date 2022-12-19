package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
    public static void main(String[] args) {

        log.info("Hellp");
        // create prdducer props
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"1");

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        //send data


        // send data
        for(int i=0;i<30;i++){
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java1", "Hnowhere"+1);
            producer.send(producerRecord, (RecordMetadata metadata, Exception exception)-> {
                if(exception == null)
                    log.info("Receievd metadata"+ metadata.partition() +","+ metadata.partition());
                else
                    log.info("Error while sending",exception);
            });
            try{
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }


        //flush and close Producer

        producer.close();

    }
}
