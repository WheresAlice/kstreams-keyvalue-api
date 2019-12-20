package kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Debug {
    public static void main(String[] args) throws Exception {
        // Configure the Kafka connection
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "keyvalue-api-debug");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Ensure keys and values are received as strings
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Build a KStream
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("keyvalue-store");

        // Print keys and values of messages received
        source.print(Printed.toSysOut());

        // Build the topology and print the description
        final Topology topology = builder.build();
        System.out.println(topology.describe());

        // Build the stream
        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
