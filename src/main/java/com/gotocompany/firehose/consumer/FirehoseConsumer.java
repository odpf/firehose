package com.gotocompany.firehose.consumer;

import java.io.Closeable;
import java.io.IOException;

public interface FirehoseConsumer extends Closeable {

    void process() throws IOException;
}
