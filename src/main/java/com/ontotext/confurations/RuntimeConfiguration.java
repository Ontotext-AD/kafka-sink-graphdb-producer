package com.ontotext.confurations;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Holds the configuration for the Kafka producer. Loads the configuration from file, then overwrites any properties
 * with those provided in the constructor argument
 */
public class RuntimeConfiguration {
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
