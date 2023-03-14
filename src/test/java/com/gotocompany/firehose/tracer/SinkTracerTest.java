package com.gotocompany.firehose.tracer;

import com.gotocompany.firehose.message.Message;
import io.opentracing.Span;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SinkTracerTest {


    private List<Message> messages;

    @Before
    public void setUp() {
        Message msg1 = new Message(new byte[]{}, new byte[]{}, "topic", 0, 100);
        Message msg2 = new Message(new byte[]{}, new byte[]{}, "topic", 0, 100);
        messages = Arrays.asList(msg1, msg2);
    }

    @Test
    public void shouldCreateSpanWhenTheHeadersIsEmptyAndTracingIsEnabled() {
        SinkTracer sinkTracer = new SinkTracer(new MockTracer(), "logSink", true);
        List<Span> spans = sinkTracer.startTrace(messages);
        assertEquals(spans.size(), 2);
        MockSpan span = (MockSpan) spans.get(0);
        assertEquals(span.operationName(), "logSink");
        assertEquals(span.tags().size(), 2);
        assertEquals(span.references().size(), 0);
        assertEquals(span.parentId(), 0);
        assertEquals(((MockTracer) sinkTracer.getTracer()).finishedSpans().size(), 0);


    }

    @Test
    public void shouldNotCreateSpanWhenTheHeadersIsEmptyAndTracingIsDisabled() {
        SinkTracer sinkTracer = new SinkTracer(new MockTracer(), "logSink", false);
        List<Span> spans = sinkTracer.startTrace(messages);
        assertEquals(spans.size(), 0);
        assertEquals(((MockTracer) sinkTracer.getTracer()).finishedSpans().size(), 0);

    }

    @Test
    public void shouldCloseTheSpan() {
        SinkTracer sinkTracer = new SinkTracer(new MockTracer(), "logSink", true);
        List<Span> spans = sinkTracer.startTrace(messages);
        sinkTracer.finishTrace(spans);
        assertEquals(((MockTracer) sinkTracer.getTracer()).finishedSpans().size(), 2);
    }
}
