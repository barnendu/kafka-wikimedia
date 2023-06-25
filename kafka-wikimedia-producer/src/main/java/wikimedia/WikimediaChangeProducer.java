package wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangeProducer {
    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeProducer.class);
    public static void main(String[] args) throws InterruptedException {

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        String bootstrapServer = "127.0.0.1:9092";
        Properties  properties = new Properties();
        // setup kafka producer
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName() );

        // setup high throughput producer

        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaEventHandler(producer, topic);

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));

        EventSource source = builder.build();

        source.start();

        TimeUnit.MINUTES.sleep(5);


    }
}
