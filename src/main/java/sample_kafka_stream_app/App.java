package sample_kafka_stream_app;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

/**
 * Hello world!
 *
 */
public class App {

    private String reqTopicName = "word-count-req";
    private String resTopicName = "word-count-res";

    public static void main(String[] args) {
        new App().SetupAndRunPipeline();
    }

    private void SetupAndRunPipeline() {
        // With High Level DSL
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "sample_kafka_stream_app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        // config.put(StreamsConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        System.out.println("Hello World!");

        StreamsBuilder builder = new StreamsBuilder();

        // split by space
        KStream<String, String> stream = builder.stream(reqTopicName);
        KTable<String, Long> wordCounts = stream.mapValues(value -> value.toLowerCase()) // lower case
                .flatMapValues(splitTextCode -> Arrays.asList(splitTextCode.split(" "))) // split by space
                .selectKey((ignoredKey, word) -> word) // so that we can group
                .groupByKey().count("counts");

        wordCounts.to(Serdes.String(), Serdes.Long(), resTopicName);

        KafkaStreams streamPipeline = new KafkaStreams(builder.build(), config);
        streamPipeline.start();
        System.out.println(streamPipeline.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streamPipeline::close)); //syntatic sugar, a timeout can also do it
    }
}
