package log.console.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) throws InterruptedException {
        log.info("I am a producer....");

        Properties  properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName() );

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int j=0 ; j<3; j++) {
            for (int i = 0; i < 10; i++) {
                // create producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("Kafka-Basic", "id_" + i, "This is something different.." + i);

                producer.send(producerRecord, (metadata, e) -> {
                    if (e == null) {
                        log.info("Received metadata from the topic \n" +
                                "Topic name:" + metadata.topic() + "\n" +
                                "Partition:" + metadata.partition() + "\n" +
                                "Offset:" + metadata.offset() + "\n" +
                                "Timestamp:" + metadata.timestamp() + "\n");
                    } else {
                        log.error("Error from kafka: " + e);
                    }
                });
            }
            Thread.sleep(500);
        }
        // send all the record and block until done -- synchronous
        producer.flush();
        // flush and close the connection
        producer.close();

    }
}
