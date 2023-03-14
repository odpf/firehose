package com.gotocompany.firehose.tracer;

import com.gotocompany.firehose.message.Message;
import io.opentracing.Span;

import java.util.List;

/**
 * An interface to trace the transactions.
 */
public interface Traceable {

    /**
     * Start trace.
     *
     * @param messages messages to trace
     * @return the list of spans
     */
    List<Span> startTrace(List<Message> messages);

    /**
     * Finish trace.
     *
     * @param spans the spans
     */
    default void finishTrace(List<Span> spans) {
        spans.forEach(span -> span.finish());
    }

}
