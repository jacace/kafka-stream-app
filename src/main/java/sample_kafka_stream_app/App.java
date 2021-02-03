package sample_kafka_stream_app;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
//import org.springframework.core.env.Environment;

/**
 * Tets of confluent cloud kafka!
 */
public class App {

    private String resTopicName = "out_topic";
    //@Autowired
    //private Environment env;    


    public static void main(String[] args) {
        new App().SetupAndRunPipeline();
    }

    private void SetupAndRunPipeline() {
        //General configuration
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, Config.ConsumerId);        
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BootstrapServers);
        config.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, Config.SecurityProtocol);
        config.put(SaslConfigs.SASL_MECHANISM, Config.SaslMechanism);
        config.put(SaslConfigs.SASL_JAAS_CONFIG, Config.SaslJaasConfig);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // config.put(StreamsConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Option #1: With High Level DSL
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(Config.TopicIn);
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


        //This recovers state (offset, where it left)
        //kafka maintains repartition and changelog topics
        //to add more partitions, repartition is required        
        //if onw instance is down, rabalance is automatic