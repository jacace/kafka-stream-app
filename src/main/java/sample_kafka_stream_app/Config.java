package sample_kafka_stream_app;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

    public static String ConsumerId;
    public static String BootstrapServers;
    public static String SecurityProtocol;
    public static String SaslMechanism;
    public static String SaslJaasConfig;
    // public static String SslEndpointIdentificationAlgorithm;
    public static String TopicOut;
    public static String TopicIn1;
    public static String TopicIn2;

    public void load() {
        try {
            String file = "application.properties";
            InputStream fins = getClass().getClassLoader().getResourceAsStream(file);
            Properties appSettings = new Properties();
            if (fins != null)
                appSettings.load(fins);

            // This is where you add your config variables:
            // ConsumerId = Boolean.parseBoolean((String) appSettings.get("DEBUG"));
            ConsumerId = (String) appSettings.get("consumerid");
            BootstrapServers = (String) appSettings.get("bootstrap.servers");
            SecurityProtocol = (String) appSettings.get("security.protocol");
            SaslMechanism = (String) appSettings.get("sasl.mechanism");
            SaslJaasConfig = (String) appSettings.get("sasl.jaas.config");
            TopicOut = (String) appSettings.get("topicout");
            fins.close();

        } catch (IOException e) {
            System.out.println("Could not load settings file.");
            System.out.println(e.getMessage());
        }

    }
}