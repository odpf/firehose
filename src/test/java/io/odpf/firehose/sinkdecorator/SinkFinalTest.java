package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.Metrics;
import io.odpf.firehose.sink.Sink;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SinkFinalTest {

    @Test
    public void shouldIgnoreMessages() throws IOException {
        Sink sink = Mockito.mock(Sink.class);
        Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
        SinkFinal sinkFinal = new SinkFinal(sink, instrumentation);
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message("".getBytes(), "".getBytes(), "", 0, 0));
            add(new Message("".getBytes(), "".getBytes(), "", 0, 0));
        }};
        Mockito.when(sink.pushMessage(messages)).thenReturn(messages);

        sinkFinal.pushMessage(messages);
        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("Ignoring messages {}", 2);
        Mockito.verify(instrumentation, Mockito.times(1)).captureGlobalMessageMetrics(Metrics.MessageScope.IGNORED, 2);
    }
}
