package kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static spark.Spark.get;

public class App {
    public static void main(String[] args) throws Exception {
        // Configure Kafka connection
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "keyvalue-api");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Create a materialized kTable
        final StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> kvTable = builder.table("keyvalue-store", Materialized.as("keyvalue-store"));

        // Build the topology and print out the description
        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        // Create a future so that we can wait for Kafka Streams to start
        CompletableFuture<KafkaStreams.State> stateFuture = new CompletableFuture<>();
        streams.setStateListener((newState, oldState) -> {
            if(stateFuture.isDone()) {
                return;
            }
            if(newState == KafkaStreams.State.RUNNING || newState == KafkaStreams.State.ERROR) {
                stateFuture.complete(newState);
            }
        });
        streams.start();

        // Handle ctrl-c
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Wait for Kafka Streams to go into a RUNNING state
        try {
            KafkaStreams.State finalState = stateFuture.get();
            if (finalState != KafkaStreams.State.RUNNING) {
                System.out.println("not running");
            }
        } catch (InterruptedException | ExecutionException ex) {
            System.out.println(ex.getMessage());
        }

        // Create a read-only Key Value store
        ReadOnlyKeyValueStore<String, String> keyValueStore =
                streams.store("keyvalue-store", QueryableStoreTypes.keyValueStore());

        System.out.println("Started up with roughly " + keyValueStore.approximateNumEntries() + " keys");
        System.out.println("Listening on http://127.0.0.1:4567");

        // Create an http server and listen for GET requests
        get("/:key", (req, res) -> keyValueStore.get(req.params(":key")));
    }
}
