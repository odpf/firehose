package io.odpf.firehose.consumer;

import io.odpf.firehose.filter.FilterException;

import java.io.Closeable;
import java.io.IOException;

public interface KafkaConsumer extends Closeable {

    void process() throws IOException, FilterException;
}
