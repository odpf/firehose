package io.odpf.firehose.sink.objectstorage.writer.local;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.exception.EglcConfigurationException;
import io.odpf.firehose.sink.objectstorage.Constants;
import io.odpf.firehose.sink.objectstorage.writer.local.policy.WriterPolicy;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

@AllArgsConstructor
public class LocalStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalStorage.class);
    private final Constants.WriterType writerType;
    private final int pageSize;
    private final int blockSize;
    private final Descriptors.Descriptor messageDescriptor;
    private final List<Descriptors.FieldDescriptor> metadataFieldDescriptor;
    private final Path basePath;
    @Getter
    private final List<WriterPolicy> policies;
    @Getter
    private final PartitionFactory partitionFactory;

    public LocalFileWriter createLocalFileWriter(Path partitionPath) {
        String fileName = UUID.randomUUID().toString();
        Path dir = basePath.resolve(partitionPath);
        Path fullPath = dir.resolve(Paths.get(fileName));

        return createWriter(fullPath);
    }

    private LocalParquetFileWriter createWriter(Path fullPath) {
        switch (writerType) {
            case PARQUET:
                try {
                    LOGGER.info("Creating Local File " + fullPath);
                    return new LocalParquetFileWriter(System.currentTimeMillis(), fullPath.toString(), pageSize, blockSize, messageDescriptor, metadataFieldDescriptor);
                } catch (IOException e) {
                    throw new LocalFileWriterFailedException(e);
                }
            default:
                throw new EglcConfigurationException("unsupported file writer type");
        }
    }

    public void deleteLocalFile(String pathString) {
        try {
            LOGGER.info("Deleting Local File " + pathString);
            Files.delete(Paths.get(pathString));
        } catch (IOException e) {
            throw new LocalFileWriterFailedException(e);
        }
    }

    public long getFileSize(String path) throws IOException {
        return Files.size(Paths.get(path));
    }

    public Boolean shouldRotate(LocalFileWriter writer) {
        return this.policies.stream().reduce(false,
                (accumulated, writerPolicy) -> accumulated || writerPolicy.shouldRotate(writer), (left, right) -> left || right);
    }
}
