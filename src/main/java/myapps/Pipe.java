package myapps;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/*
 * Sample using the Generic Avro Serializer / Deserializer (Serde)
 */
public class Pipe {

  // A mocked schema registry for our serdes to use
  private static final String SCHEMA_REGISTRY_SCOPE = GenericAvroIntegrationTest.class.getName();
  private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

  private static String inputTopic = "inputTopic";
  private static String outputTopic = "outputTopic";

  private static  List<Object> generateSampleData()
  {
    final GenericRecord record = new GenericData.Record(schema);
    record.put("user", "alice");
    record.put("is_new", true);
    record.put("content", "lorem ipsum");
    return Collections.singletonList(record);
  }  

  public static void main(String[] args) throws Exception {

    final Schema schema = new Schema.Parser().parse(
      new Pipe().getClass().getResourceAsStream("/resources/sample_schema.avsc")
    );
    
    final SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);
    schemaRegistryClient.register("inputTopic-value", schema);   

    //
    // Step 1: Configure and start the processor topology.
    //
    final StreamsBuilder builder = new StreamsBuilder();
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "generic-avro-serde");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
    streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

    final Serde<String> stringSerde = Serdes.String();
    final Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();


    genericAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL),
      /*isKey*/ false);
    final KStream<String, GenericRecord> stream = builder.stream(inputTopic);
    stream.to(outputTopic, Produced.with(stringSerde, genericAvroSerde));

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)){
      //
      // Step 2: Setup input and output topics.
      //
      final TestInputTopic<Void, Object> input = topologyTestDriver
        .createInputTopic(inputTopic,
                          new IntegrationTestUtils.NothingSerde<>(),
                          new KafkaAvroSerializer(schemaRegistryClient));
      final TestOutputTopic<Void, Object> output = topologyTestDriver
        .createOutputTopic(outputTopic,
                           new IntegrationTestUtils.NothingSerde<>(),
                           new KafkaAvroDeserializer(schemaRegistryClient));

      //
      // Step 3: Produce some input data to the input topic.
      //
      final List<Object> inputValues = generateSampleData();
      input.pipeValueList(finputValues );

      //
      // Step 4: Verify the application's output data.
      //
      assertThat(output.readValuesToList(), equalTo(inputValues));
    } finally {
      MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
    }
  }
  
}