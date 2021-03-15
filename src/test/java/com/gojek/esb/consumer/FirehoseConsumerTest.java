package com.gojek.esb.consumer;

import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.filter.FilterException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.tracer.SinkTracer;
import com.gojek.esb.util.Clock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseConsumerTest {

    @Mock
    private GenericConsumer genericConsumer;

    @Mock
    private Sink sink;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private Clock clock;

    @Mock
    private SinkTracer tracer;

    private FirehoseConsumer firehoseConsumer;
    private List<Message> messages;

    @Before
    public void setUp() throws Exception {
        Message msg1 = new Message(new byte[]{}, new byte[]{}, "topic", 0, 100);
        Message msg2 = new Message(new byte[]{}, new byte[]{}, "topic", 0, 100);
        messages = Arrays.asList(msg1, msg2);

        firehoseConsumer = new FirehoseConsumer(genericConsumer, sink, clock, tracer, instrumentation);

        when(genericConsumer.readMessages()).thenReturn(messages);
        when(clock.now()).thenReturn(Instant.now());
    }

    @Test
    public void shouldProcessPartitions() throws IOException, DeserializerException, FilterException {
        firehoseConsumer.processPartitions();

        verify(sink).pushMessage(messages);
    }

    @Test
    public void shouldProcessEmptyPartitions() throws IOException, DeserializerException, FilterException {
        when(genericConsumer.readMessages()).thenReturn(new ArrayList<>());

        firehoseConsumer.processPartitions();

        verify(sink, times(0)).pushMessage(anyList());
    }

    @Test
    public void shouldSendNoOfMessagesReceivedCount() throws IOException, DeserializerException, FilterException {
        firehoseConsumer.processPartitions();
        verify(instrumentation).logInfo("Execution successful for {} records", 2);
    }

    @Test
    public void shouldSendPartitionProcessingTime() throws IOException, DeserializerException, FilterException {
        Instant beforeCall = Instant.now();
        Instant afterCall = beforeCall.plusSeconds(1);
        when(clock.now()).thenReturn(beforeCall).thenReturn(afterCall);
        firehoseConsumer.processPartitions();
        verify(instrumentation).captureDurationSince("firehose_source_kafka_partitions_process_milliseconds", beforeCall);
    }

    @Test
    public void shouldCallTracerWithTheSpan() throws IOException, DeserializerException, FilterException {
        firehoseConsumer.processPartitions();

        verify(sink).pushMessage(messages);
        verify(tracer).startTrace(messages);
        verify(tracer).finishTrace(any());
    }

    @Test
    public void shouldCloseConsumerIfConsumerIsNotNull() throws IOException {
        firehoseConsumer.close();

        verify(instrumentation, times(1)).logInfo("closing consumer");
        verify(tracer, times(1)).close();
        verify(genericConsumer, times(1)).close();

        verify(sink, times(1)).close();
        verify(instrumentation, times(1)).close();
    }

    @Test
    public void shouldNotCloseConsumerIfConsumerIsNull() throws IOException {

        firehoseConsumer = new FirehoseConsumer(null, sink, clock, tracer, instrumentation);
        firehoseConsumer.close();

        verify(instrumentation, times(0)).logInfo("closing consumer");
        verify(tracer, times(0)).close();
        verify(genericConsumer, times(0)).close();

        verify(sink, times(1)).close();
        verify(instrumentation, times(1)).close();
    }
}
