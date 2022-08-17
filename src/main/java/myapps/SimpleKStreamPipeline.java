package myapps;

import io.confluent.common.utils.TestUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.*;

public class SimpleKStreamPipeline {

    static final String inputTopic = "xxx";
    static final String outputTopic = "ooo";

    public static void main(final String[] args) {
        Map<String, String> env = System.getenv();
        String bootstrapServers = env.get("DEMO_BOOTSTRAP_SERVER");
        if (bootstrapServers == null) {
            bootstrapServers = "localhost:9092";
        }

        final Properties streamsConfiguration = getStreamsConfiguration(bootstrapServers);

        final StreamsBuilder builder = new StreamsBuilder();
        createStream(builder);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                System.out.println("***********");
                for (ThreadMetadata tmd : streams.metadataForLocalThreads()) {
                    System.out.println("Thread "
                            + tmd.threadName()
                            + " active "
                            + Arrays.toString(tmd.activeTasks().stream().map(val -> val.taskId()).toArray())
                            + " standby "
                            + Arrays.toString(tmd.standbyTasks().stream().map(val -> val.taskId()).toArray()));
                }
            }
        });

        streams.cleanUp();
        streams.start();


        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static Properties getStreamsConfiguration(final String bootstrapServers) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "xx");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "xx");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 6);
        streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
        return streamsConfiguration;
    }

    static void createStream(final StreamsBuilder builder) {

        builder.stream(inputTopic)
                .groupByKey()
                .count(Materialized.as("count-store"))
                .toStream()
                .to(outputTopic);
    }
}