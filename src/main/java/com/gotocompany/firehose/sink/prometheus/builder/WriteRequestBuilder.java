package com.gotocompany.firehose.sink.prometheus.builder;


import com.gotocompany.firehose.message.Message;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import cortexpb.Cortex;
import com.gotocompany.stencil.Parser;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Builder for Cortex WriteRequest.
 */
public class WriteRequestBuilder {

    private Cortex.WriteRequest.Builder writeRequestBuilder = Cortex.WriteRequest.newBuilder();
    private TimeSeriesBuilder timeSeriesBuilder;
    private Parser protoParser;

    /**
     * Instantiates a new Write request builder.
     *
     * @param timeSeriesBuilder the TimeSeriesBuilder
     * @param protoParser       the proto parser
     */
    public WriteRequestBuilder(TimeSeriesBuilder timeSeriesBuilder, Parser protoParser) {
        this.timeSeriesBuilder = timeSeriesBuilder;
        this.protoParser = protoParser;
    }

    /**
     * build a cortex write request object.
     *
     * @param messages                          the list of consumer message
     * @return                                  Cortex.WriteRequest
     * @throws InvalidProtocolBufferException   the exception on invalid protobuf
     */
    public Cortex.WriteRequest buildWriteRequest(List<Message> messages) throws InvalidProtocolBufferException {
        writeRequestBuilder.clear();
        List<Cortex.TimeSeries> sortedTimeSeriesList = new ArrayList<>();
        for (Message message : messages) {
            DynamicMessage protoMessage = protoParser.parse(message.getLogMessage());
            int partition = message.getPartition();
            sortedTimeSeriesList.addAll(timeSeriesBuilder.buildTimeSeries(protoMessage, partition));
        }
        sortedTimeSeriesList.sort(Comparator.comparing(o -> o.getSamplesList().get(0).getTimestampMs()));
        writeRequestBuilder.addAllTimeseries(sortedTimeSeriesList);
        return writeRequestBuilder.build();
    }
}
