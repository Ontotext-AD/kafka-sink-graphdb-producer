package com.ontotext.confurations;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Class used to access the demonstrator configurations.
 */
public class RuntimeConfiguration {
    public static final String VERSION = "Kafka Sink Producer 1.0";
    private final Properties properties;

    public RuntimeConfiguration(Map<String, String> propsMap) {
        properties = new Properties();
        try {
            properties.load(RuntimeConfiguration.class.getClassLoader().getResourceAsStream("graphdb-kafka-sink.properties"));
            properties.putAll(propsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Properties getProperties() {
        return properties;
    }
}
