package com.gotocompany.firehose.tracer;

import com.gotocompany.firehose.message.Message;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaUtils;
import io.opentracing.tag.Tags;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Sink Tracer.
 */
@AllArgsConstructor
@Getter
public class SinkTracer implements Traceable, Closeable {
    private Tracer tracer;
    private String name;
    private boolean enabled;

    @Override
    public List<Span> startTrace(List<Message> messages) {
        if (enabled) {
            return messages.stream().map(m -> traceMessage(m)).collect(Collectors.toList());
        } else {
            return new ArrayList<>();
        }
    }

    private Span traceMessage(Message message) {
        SpanContext parentContext = null;
        if (message.getHeaders() != null) {
            parentContext = TracingKafkaUtils.extractSpanContext(message.getHeaders(), tracer);
        }

        Tracer.SpanBuilder spanBuilder = tracer
                .buildSpan(name)
                .withTag(Tags.COMPONENT, name)
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);

        if (parentContext != null) {
            spanBuilder.addReference(References.FOLLOWS_FROM, parentContext);
        }
        return spanBuilder.start();

    }

    @Override
    public void close() {
        tracer.close();
    }
}
