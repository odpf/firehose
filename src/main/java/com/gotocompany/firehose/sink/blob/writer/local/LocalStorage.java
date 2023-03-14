package com.gotocompany.firehose.sink.blob.writer.local;

import com.google.protobuf.Descriptors;
import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.exception.ConfigurationException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.blob.writer.local.policy.WriterPolicy;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

@AllArgsConstructor
public class LocalStorage {

    private final BlobSinkConfig sinkConfig;
    private final Descriptors.Descriptor messageDescriptor;
    private final List<Descriptors.FieldDescriptor> metadataFieldDescriptor;
    private final List<WriterPolicy> policies;
    private final FirehoseInstrumentation firehoseInstrumentation;

    public LocalFileWriter createLocalFileWriter(Path partitionPath) {
        Path basePath = Paths.get(sinkConfig.getLocalDirectory());
        String fileName = UUID.randomUUID().toString();
        Path dir = basePath.resolve(partitionPath);
        Path fullPath = dir.resolve(Paths.get(fileName));
        return createWriter(basePath, fullPath);
    }

    private LocalParquetFileWriter createWriter(Path basePath, Path fullPath) {
        switch (sinkConfig.getLocalFileWriterType()) {
            case PARQUET:
                try {
                    firehoseInstrumentation.logInfo("Creating Local File " + fullPath);
                    return new LocalParquetFileWriter(
                            System.currentTimeMillis(),
                            basePath.toString(),
                            fullPath.toString(),
                            sinkConfig,
                            messageDescriptor,
                            metadataFieldDescriptor);
                } catch (IOException e) {
                    throw new LocalFileWriterFailedException(e);
                }
            default:
                throw new ConfigurationException("unsupported file writer type");
        }
    }

    public void deleteLocalFile(String pathString) {
        switch (sinkConfig.getLocalFileWriterType()) {
            case PARQUET:
                try {
                    Path filePath = Paths.get(pathString);
                    Path crcFilePath = filePath.getParent().resolve("." + filePath.getFileName() + ".crc");
                    firehoseInstrumentation.logInfo("Deleting Local File {}", filePath);
                    firehoseInstrumentation.logInfo("Deleting Local File {}", crcFilePath);
                    deleteLocalFile(filePath, crcFilePath);
                } catch (IOException e) {
                    throw new LocalFileWriterFailedException(e);
                }
                break;
            default:
                throw new ConfigurationException("unsupported file writer type");
        }
    }

    public void deleteLocalFile(Path... paths) throws IOException {
        for (Path path : paths) {
            Files.delete(path);
        }
    }

    public Boolean shouldRotate(LocalFileWriter writer) {
        return policies.stream().anyMatch(writerPolicy -> writerPolicy.shouldRotate(writer.getMetadata()));
    }
}
