package io.odpf.firehose.sink.objectstorage.message;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProto;
import io.odpf.firehose.sink.objectstorage.proto.NestedKafkaMetadataProto;
import org.junit.Test;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;


public class KafkaMetadataUtilsTest {

    private final String topic = "default";
    private final long offset = 1L;
    private final int partition = 1;
    private final byte[] logKey = "key".getBytes();
    private final byte[] logMessage = "value".getBytes();
    private final Instant timestamp = Instant.parse("2021-01-01T00:00:00.000Z");
    private final Instant consumeTimestamp = Instant.parse("2021-01-01T10:00:00.000Z");

    @Test
    public void shouldCreateKafkaMetadataDynamicMessage() {
        String kafkaMetadataColumnName = "";

        Message message = new Message(logKey, logMessage, topic, partition, offset, null, timestamp.toEpochMilli(), consumeTimestamp.toEpochMilli());
        KafkaMetadataUtils kafkaMetadataUtils = new KafkaMetadataUtils(kafkaMetadataColumnName);
        DynamicMessage kafkaMetadata = kafkaMetadataUtils.createKafkaMetadata(message);

        Descriptors.Descriptor descriptor = kafkaMetadataUtils.getMetadataDescriptor();

        assertEquals(topic, kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProto.MESSAGE_TOPIC_FIELD_NAME)));
        assertEquals(partition, kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProto.MESSAGE_PARTITION_FIELD_NAME)));
        assertEquals(offset, kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProto.MESSAGE_OFFSET_FIELD_NAME)));
        assertEquals(Timestamp.newBuilder()
                        .setSeconds(timestamp.getEpochSecond())
                        .setNanos(timestamp.getNano()).build(),
                kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProto.MESSAGE_TIMESTAMP_FIELD_NAME)));
        assertThat(kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProto.LOAD_TIME_FIELD_NAME)), is(instanceOf(Timestamp.class)));
    }

    @Test
    public void shouldCreateKafkaDynamicMessageWhenMetadataIsNested() {
        String kafkaMetadataColumnName = "metadata_field_name";

        Message message = new Message(logKey, logMessage, topic, partition, offset, null, timestamp.toEpochMilli(), consumeTimestamp.toEpochMilli());
        KafkaMetadataUtils kafkaMetadataUtils = new KafkaMetadataUtils(kafkaMetadataColumnName);
        DynamicMessage nestedKafkaMetadata = kafkaMetadataUtils.createKafkaMetadata(message);
        Descriptors.Descriptor nestedMetadataDescriptor = kafkaMetadataUtils.getMetadataDescriptor();

        DynamicMessage kafkaMetadata = (DynamicMessage) nestedKafkaMetadata.getField(nestedMetadataDescriptor.findFieldByName(kafkaMetadataColumnName));
        Descriptors.Descriptor descriptor = kafkaMetadata.getDescriptorForType();

        assertEquals(topic, kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProto.MESSAGE_TOPIC_FIELD_NAME)));
        assertEquals(partition, kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProto.MESSAGE_PARTITION_FIELD_NAME)));
        assertEquals(offset, kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProto.MESSAGE_OFFSET_FIELD_NAME)));
        assertEquals(Timestamp.newBuilder()
                        .setSeconds(timestamp.getEpochSecond())
                        .setNanos(timestamp.getNano()).build(),
                kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProto.MESSAGE_TIMESTAMP_FIELD_NAME)));
        assertThat(kafkaMetadata.getField(descriptor.findFieldByName(KafkaMetadataProto.LOAD_TIME_FIELD_NAME)), is(instanceOf(Timestamp.class)));
    }

    @Test
    public void shouldReturnMetadataFieldDescriptor() {
        String kafkaMetadataColumnName = "";

        KafkaMetadataUtils kafkaMetadataUtils = new KafkaMetadataUtils(kafkaMetadataColumnName);
        List<Descriptors.FieldDescriptor> fieldDescriptor = kafkaMetadataUtils.getFieldDescriptor();

        assertEquals(kafkaMetadataUtils.getMetadataDescriptor().getFields(), fieldDescriptor);
    }

    @Test
    public void shouldReturnMetadataFieldDescriptorWhenMetadataIsNested() {
        String kafkaMetadataColumnName = "metadata_column_name";

        KafkaMetadataUtils kafkaMetadataUtils = new KafkaMetadataUtils(kafkaMetadataColumnName);
        List<Descriptors.FieldDescriptor> fieldDescriptor = kafkaMetadataUtils.getFieldDescriptor();

        assertEquals(kafkaMetadataUtils.getMetadataDescriptor().getFields(), fieldDescriptor);
    }

    @Test
    public void shouldReturnMetadataDescriptor() {
        String kafkaMetadataColumnName = "";

        KafkaMetadataUtils kafkaMetadataUtils = new KafkaMetadataUtils(kafkaMetadataColumnName);
        Descriptors.Descriptor descriptor = kafkaMetadataUtils.getMetadataDescriptor();

        assertEquals(KafkaMetadataProto.getTypeName(), descriptor.getName());
    }

    @Test
    public void shouldReturnMetadataDescriptorWhenMetadataIsNested() {
        String kafkaMetadataColumnName = "metadata_column_name";

        KafkaMetadataUtils kafkaMetadataUtils = new KafkaMetadataUtils(kafkaMetadataColumnName);
        Descriptors.Descriptor descriptor = kafkaMetadataUtils.getMetadataDescriptor();

        assertEquals(NestedKafkaMetadataProto.getTypeName(), descriptor.getName());
    }
}
