package com.gotocompany.firehose.parser;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class KafkaEnvironmentVariablesTest {

    @Test
    public void shouldReturnKafkaEnvVarsPositive() {
        HashMap<String, String> systemEnvs = new HashMap<String, String>() {{
            put("PATH", "/usr/local/bin");
            put("SHELL", "/usr/local/bin/zsh");
            put("source_kafka_consumer_config_fetch_min_bytes", "1");
            put("source_kafka_consumer_config_ssl_keystore_location", "/home/user/.ssh/keystore");
            put("SOURCE_KAFKA_CONSUMER_CONFIG_ENABLE_AUTO_COMMIT", "false");

        }};

        Map<String, String> expectedEnvVars = new HashMap<String, String>() {{
            put("fetch.min.bytes", "1");
            put("ssl.keystore.location", "/home/user/.ssh/keystore");
            put("enable.auto.commit", "false");
        }};

        Map<String, String> actualEnvVars = KafkaEnvironmentVariables.parse(systemEnvs);

        assertEquals(expectedEnvVars, actualEnvVars);
    }

    @Test
    public void shouldReturnKafkaEnvVarsNegative() {
        HashMap<String, String> systemEnvs = new HashMap<String, String>() {{
            put("PATH", "/usr/local/bin");
            put("SHELL", "/usr/local/bin/zsh");
        }};

        Map<String, String> expectedEnvVars = new HashMap<>();

        Map<String, String> actualEnvVars = KafkaEnvironmentVariables.parse(systemEnvs);

        assertEquals(expectedEnvVars, actualEnvVars);
    }

    @Test
    public void shouldReturnEmptyCollectionOnNullEnvVars() {
        HashMap<String, String> systemEnvs = null;
        Map<String, String> expectedEnvVars = new HashMap<>();

        Map<String, String> actualEnvVars = KafkaEnvironmentVariables.parse(systemEnvs);

        assertEquals(expectedEnvVars, actualEnvVars);
    }

    @Test
    public void shouldReturnEmptyCollectionOnEmptyEnvVars() {
        HashMap<String, String> systemEnvs = new HashMap<>();
        Map<String, String> expectedEnvVars = new HashMap<>();

        Map<String, String> actualEnvVars = KafkaEnvironmentVariables.parse(systemEnvs);

        assertEquals(expectedEnvVars, actualEnvVars);
    }
}
