package io.odpf.firehose.sinkdecorator.dlq.objectstorage;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.objectstorage.ObjectStorageException;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class ObjectStorageDlqWriter implements DlqWriter {

    private final ObjectStorage objectStorage;
    private final Gson gson;

    public ObjectStorageDlqWriter(ObjectStorage objectStorage) {
        this.objectStorage = objectStorage;
        gson = new Gson().newBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
    }

    @Override
    public List<Message> write(List<Message> messages) throws IOException {
        Map<Path, List<Message>> messagesByPartition = groupByPartition(messages);

        List<Message> unProcessedMessages = new LinkedList<>();
        for (Map.Entry<Path, List<Message>> entry : messagesByPartition.entrySet()) {
            List<DlqMessage> dlqMessages = createDlqMessages(entry.getValue());
            String dlqJson = serialise(dlqMessages);

            String fileName = UUID.randomUUID().toString();
            String objectName = entry.getKey().resolve(fileName).toString();

            try {
                objectStorage.store(objectName, dlqJson.getBytes(StandardCharsets.UTF_8));
            } catch (ObjectStorageException e) {
                throw new IOException(e);
            }
        }

        return unProcessedMessages;
    }

    private Map<Path, List<Message>> groupByPartition(List<Message> messages) {
        return messages.stream()
                .collect(Collectors.groupingBy(ObjectStorageDlqWriter::createPartition));
    }

    private List<DlqMessage> createDlqMessages(List<Message> messages) {
        List<DlqMessage> dlqMessages = messages.stream().map(message -> {
            String error = "";
            ErrorInfo errorInfo = message.getErrorInfo();
            if (errorInfo != null) {
                if (errorInfo.getErrorType() != null) {
                    error = errorInfo.getErrorType().toString();
                }
            }

            return new DlqMessage(new String(message.getLogKey()),
                    new String(message.getLogMessage()),
                    message.getTopic(),
                    message.getPartition(),
                    message.getOffset(),
                    message.getConsumeTimestamp(),
                    error);
        }).collect(Collectors.toList());
        return dlqMessages;
    }

    public String serialise(List<DlqMessage> messages) {
        List<String> jsonMessages = messages.stream()
                .map(gson::toJson)
                .collect(Collectors.toList());
        return String.join("\n", jsonMessages);
    }

    private static Path createPartition(Message message) {
        LocalDate consumeLocalDate = LocalDate.from(Instant.ofEpochMilli(message.getConsumeTimestamp())
                .atZone(ZoneId.of("UTC")));

        String consumeDate = DateTimeFormatter.ISO_LOCAL_DATE.format(consumeLocalDate);
        return Paths.get(message.getTopic(), consumeDate);
    }
}
