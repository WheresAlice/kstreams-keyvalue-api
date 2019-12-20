package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        // Configure Kafka connection
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig);

        // Create some records to produce
        final ProducerRecord<String, String> alice = new ProducerRecord<>("keyvalue-store", "alice", "work");
        final ProducerRecord<String, String> bob = new ProducerRecord<>("keyvalue-store", "bob", "home");
        final ProducerRecord<String, String> carol = new ProducerRecord<>("keyvalue-store", "carol", "school");

        // Send the records
        producer.send(alice);
        producer.send(bob);
        producer.send(carol);

        // Flush and close the connection
        producer.flush();
        producer.close();
    }
}
