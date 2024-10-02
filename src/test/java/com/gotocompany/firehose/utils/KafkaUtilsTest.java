package com.gotocompany.firehose.utils;

import com.gotocompany.firehose.config.DlqKafkaProducerConfig;
import org.aeonbits.owner.ConfigFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.types.Password;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaUtilsTest {

    private static final String DLQ_KAFKA_ACKS = "DLQ_KAFKA_ACKS";
    private static final String DLQ_KAFKA_RETRIES = "DLQ_KAFKA_RETRIES";
    private static final String DLQ_KAFKA_BATCH_SIZE = "DLQ_KAFKA_BATCH_SIZE";
    private static final String DLQ_KAFKA_LINGER_MS = "DLQ_KAFKA_LINGER_MS";
    private static final String DLQ_KAFKA_BUFFER_MEMORY = "DLQ_KAFKA_BUFFER_MEMORY";
    private static final String DLQ_KAFKA_KEY_SERIALIZER = "DLQ_KAFKA_KEY_SERIALIZER";
    private static final String DLQ_KAFKA_VALUE_SERIALIZER = "DLQ_KAFKA_VALUE_SERIALIZER";
    private static final String DLQ_KAFKA_BROKERS = "DLQ_KAFKA_BROKERS";
    private static final String DLQ_KAFKA_TOPIC = "DLQ_KAFKA_TOPIC";
    private static final String DLQ_KAFKA_SECURITY_PROTOCOL = "dlQ_KaFKa_SeCuRITY_proTOCOL";
    private static final String DLQ_KAFKA_SSL_TRUSTSTORE_PASSWORD_CONFIG = "DLQ_KAFKA_SSL_TRUSTSTORE_PASSWORD";
    private static final String DLQ_KAFKA_SASL_MECHANISM = "DLQ_KAFKA_SASL_MECHANISM";
    private static final String DLQ_KAFKA_SASL_JAAS_CONFIG = "DLQ_KAFKA_SASL_JAAS_CONFIG";

    @Test
    public void createShouldReturnKafkaProducerWithCorrectProperties() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> properties = getDlqProperties();
        DlqKafkaProducerConfig dlqKafkaProducerConfig = ConfigFactory.create(DlqKafkaProducerConfig.class, properties);

        KafkaProducer<byte[], byte[]> kafkaProducer = KafkaUtils.getKafkaProducer(KafkaProducerTypesMetadata.DLQ, dlqKafkaProducerConfig, properties);
        Field producerConfigField = KafkaProducer.class.getDeclaredField("producerConfig");
        producerConfigField.setAccessible(true);
        ProducerConfig producerConfig = (ProducerConfig) producerConfigField.get(kafkaProducer);

        assertEquals(properties.get(DLQ_KAFKA_ACKS), producerConfig.getString("acks"));
        assertEquals(properties.get(DLQ_KAFKA_RETRIES), String.valueOf(producerConfig.getInt("retries")));
        assertEquals(properties.get(DLQ_KAFKA_BATCH_SIZE), String.valueOf(producerConfig.getInt("batch.size")));
        assertEquals(properties.get(DLQ_KAFKA_LINGER_MS), String.valueOf(producerConfig.getLong("linger.ms")));
        assertEquals(properties.get(DLQ_KAFKA_BUFFER_MEMORY), String.valueOf(producerConfig.getLong("buffer.memory")));
        assertEquals(properties.get(DLQ_KAFKA_KEY_SERIALIZER), producerConfig.getClass("key.serializer").getName());
        assertEquals(properties.get(DLQ_KAFKA_VALUE_SERIALIZER), producerConfig.getClass("value.serializer").getName());
        assertEquals(Arrays.asList(properties.get(DLQ_KAFKA_BROKERS).split(",")), producerConfig.getList("bootstrap.servers"));
        assertEquals(properties.get(DLQ_KAFKA_SECURITY_PROTOCOL), producerConfig.getString("security.protocol"));
        assertEquals(properties.get(DLQ_KAFKA_SSL_TRUSTSTORE_PASSWORD_CONFIG), producerConfig.getString("ssl.truststore.password"));
        assertEquals(properties.get(DLQ_KAFKA_SASL_MECHANISM), producerConfig.getString("sasl.mechanism"));
        assertEquals(new Password(properties.get(DLQ_KAFKA_SASL_JAAS_CONFIG)), producerConfig.getPassword("sasl.jaas.config"));
    }

    @Test
    public void createShouldIgnoreParametersNotMatchingPrefix() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> properties = getDlqProperties();
        properties.put("DLQ_KAFKACLIENT_ID", "clientId");
        properties.put("NOTDLQ_KAFKA_CLIENT_ID", "clientId");
        DlqKafkaProducerConfig kafkaProducerConfig = ConfigFactory.create(DlqKafkaProducerConfig.class, properties);

        KafkaProducer<byte[], byte[]> kafkaProducer = KafkaUtils.getKafkaProducer(KafkaProducerTypesMetadata.DLQ, kafkaProducerConfig, properties);
        Field producerConfigField = KafkaProducer.class.getDeclaredField("producerConfig");
        producerConfigField.setAccessible(true);
        ProducerConfig producerConfig = (ProducerConfig) producerConfigField.get(kafkaProducer);

        assertEquals(properties.get(DLQ_KAFKA_ACKS), producerConfig.getString("acks"));
        assertEquals(properties.get(DLQ_KAFKA_RETRIES), String.valueOf(producerConfig.getInt("retries")));
        assertEquals(properties.get(DLQ_KAFKA_BATCH_SIZE), String.valueOf(producerConfig.getInt("batch.size")));
        assertEquals(properties.get(DLQ_KAFKA_LINGER_MS), String.valueOf(producerConfig.getLong("linger.ms")));
        assertEquals(properties.get(DLQ_KAFKA_BUFFER_MEMORY), String.valueOf(producerConfig.getLong("buffer.memory")));
        assertEquals(properties.get(DLQ_KAFKA_KEY_SERIALIZER), producerConfig.getClass("key.serializer").getName());
        assertEquals(properties.get(DLQ_KAFKA_VALUE_SERIALIZER), producerConfig.getClass("value.serializer").getName());
        assertEquals(Arrays.asList(properties.get(DLQ_KAFKA_BROKERS).split(",")), producerConfig.getList("bootstrap.servers"));
        assertEquals(properties.get(DLQ_KAFKA_SECURITY_PROTOCOL), producerConfig.getString("security.protocol"));
        assertEquals(properties.get(DLQ_KAFKA_SSL_TRUSTSTORE_PASSWORD_CONFIG), producerConfig.getString("ssl.truststore.password"));
        assertEquals(properties.get(DLQ_KAFKA_SASL_MECHANISM), producerConfig.getString("sasl.mechanism"));
        assertEquals(new Password(properties.get(DLQ_KAFKA_SASL_JAAS_CONFIG)), producerConfig.getPassword("sasl.jaas.config"));
        assertTrue(StringUtils.isEmpty(producerConfig.getString("client.id")));

    }

    private static Map<String, String> getDlqProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(DLQ_KAFKA_ACKS, "all");
        properties.put(DLQ_KAFKA_RETRIES, "2147483647");
        properties.put(DLQ_KAFKA_BATCH_SIZE, "16384");
        properties.put(DLQ_KAFKA_LINGER_MS, "0");
        properties.put(DLQ_KAFKA_BUFFER_MEMORY, "33554432");
        properties.put(DLQ_KAFKA_KEY_SERIALIZER, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(DLQ_KAFKA_VALUE_SERIALIZER, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(DLQ_KAFKA_BROKERS, "localhost:9092");
        properties.put(DLQ_KAFKA_TOPIC, "firehose-retry-topic");
        properties.put(DLQ_KAFKA_SECURITY_PROTOCOL, "SASL_SSL");
        properties.put(DLQ_KAFKA_SASL_MECHANISM, "OAUTHBEARER");
        properties.put(DLQ_KAFKA_SASL_JAAS_CONFIG, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        return properties;
    }
}
