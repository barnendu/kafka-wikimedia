package log.console.kafka.opensearch;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class OpenSearchConsumer {
    public static RestHighLevelClient createOpenSearchClient(){
        String url = "http://localhost:9200";

        URI connUri = URI.create(url);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort() )));

        return restHighLevelClient;
    }

    public static KafkaConsumer<String,String> createKafkaCOnsumer(){
        String groupId = "kafka-wikimedia-grp";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName() );
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        return new KafkaConsumer<>(properties);
    }
    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class);

        RestHighLevelClient openSearchClient = createOpenSearchClient();

        KafkaConsumer<String, String> consumer = createKafkaCOnsumer();

        try(openSearchClient; consumer){
            consumer.subscribe(Arrays.asList("wikimedia.recentchange"));
            boolean indexExist = openSearchClient.indices().exists( new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if(!indexExist) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Index successfully  created...");
            }else{
                log.info("index already exist...");
            }
            while (true) {
                ConsumerRecords<String, String > records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("received " +recordCount+ " record(s)");

                for( ConsumerRecord<String, String> record : records){
                    IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON);
                    IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                    log.info(response.getId());
                }

            }
        }
    }
}
