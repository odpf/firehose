package io.odpf.firehose.sink.objectstorage;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.StringValue;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.sink.objectstorage.message.KafkaMetadataUtils;
import io.odpf.firehose.sink.objectstorage.message.MessageProto;
import io.odpf.firehose.sink.objectstorage.message.Record;

import java.time.Instant;

public class TestUtils {

    public static Record createRecordWithMetadata(String msgValue, String topic, int partition, long offset, Instant timestamp) {
        Message message = new Message("".getBytes(), msgValue.getBytes(), topic, partition, offset, null, timestamp.toEpochMilli(), timestamp.toEpochMilli());
        KafkaMetadataUtils metadataFactory = new KafkaMetadataUtils("");
        DynamicMessage kafkaMetadata = metadataFactory.createKafkaMetadata(message);
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(StringValue.of(msgValue)).build();
        return new Record(dynamicMessage, kafkaMetadata);
    }

    public static DynamicMessage createMetadata(String kafkaMetadataFieldName, Instant eventTimestamp, long offset, int partition, String topic) {
        Message message = new Message("".getBytes(), "".getBytes(), topic, partition, offset, null, eventTimestamp.toEpochMilli(), eventTimestamp.toEpochMilli());
        KafkaMetadataUtils metadataFactory = new KafkaMetadataUtils(kafkaMetadataFieldName);
        return metadataFactory.createKafkaMetadata(message);
    }

    public static DynamicMessage createMessage(Instant timestamp, int orderNum) {
        MessageProto.MessageBuilder messageBuilder = MessageProto.createMessageBuilder();
        return messageBuilder
                .setCreatedTime(timestamp)
                .setOrderNumber(orderNum).build();
    }
}
