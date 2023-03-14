package com.gotocompany.firehose.sink.prometheus.builder;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.gotocompany.firehose.sink.prometheus.PromSinkConstants;
import com.gotocompany.firehose.consumer.TestFeedbackLogMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class TimeSeriesBuilderUtilsTest {

    @Test
    public void shouldGetMetricListFromMessage() throws InvalidProtocolBufferException {
        Properties metricNameProtoIndexMapping = new Properties();
        metricNameProtoIndexMapping.setProperty("7", "tip_amount");
        metricNameProtoIndexMapping.setProperty("5", "feedback_ratings");
        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setFeedbackRating(5)
                .setCustomerId("customer-id")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000)).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());
        List<PrometheusMetric> metricsFromMessage = new ArrayList<>(TimeSeriesBuilderUtils.getMetricsFromMessage(dynamicMessage, metricNameProtoIndexMapping));
        Assert.assertEquals(2, metricsFromMessage.size());
        metricsFromMessage.sort(Comparator.comparing(PrometheusMetric::getName));
        Assert.assertEquals("feedback_ratings", metricsFromMessage.get(0).getName());
        Assert.assertEquals(5.0, metricsFromMessage.get(0).getValue(), 0.0001);
        Assert.assertEquals("tip_amount", metricsFromMessage.get(1).getName());
        Assert.assertEquals(10000.0, metricsFromMessage.get(1).getValue(), 0.0001);
    }


    @Test
    public void shouldGetLabelsFromMessage() throws InvalidProtocolBufferException {
        Properties labelNameProtoIndexMapping = new Properties();
        labelNameProtoIndexMapping.setProperty("3", "driver_id");
        labelNameProtoIndexMapping.setProperty("4", "customer_id");
        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setFeedbackRating(5)
                .setCustomerId("CUSTOMER")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000)).build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());
        Set<PrometheusLabel> labelsFromMessage = TimeSeriesBuilderUtils.getLabelsFromMessage(dynamicMessage, labelNameProtoIndexMapping, 1);
        Assert.assertEquals(3, labelsFromMessage.size());
        Assert.assertTrue(labelsFromMessage.contains(new PrometheusLabel("driver_id", "DRIVER")));
        Assert.assertTrue(labelsFromMessage.contains(new PrometheusLabel("customer_id", "CUSTOMER")));
        Assert.assertTrue(labelsFromMessage.contains(new PrometheusLabel(PromSinkConstants.KAFKA_PARTITION, "1")));
    }

    @Test
    public void shouldGetNestedMetricListFromMessage() throws InvalidProtocolBufferException {
        Properties metricNameProtoIndexMapping = new Properties();
        Properties m = new Properties();
        m.setProperty("1", "order_completion_time_seconds");
        metricNameProtoIndexMapping.put("15", m);
        long seconds = System.currentTimeMillis() / 1000;
        TestFeedbackLogMessage feedbackLogMessage =
                TestFeedbackLogMessage.newBuilder().
                        setOrderCompletionTime(Timestamp.newBuilder().setSeconds(seconds)).build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());
        List<PrometheusMetric> metricsFromMessage = new ArrayList<>(TimeSeriesBuilderUtils.getMetricsFromMessage(dynamicMessage, metricNameProtoIndexMapping));
        Assert.assertEquals(1, metricsFromMessage.size());
        Assert.assertEquals("order_completion_time_seconds", metricsFromMessage.get(0).getName());
        Assert.assertEquals(seconds, metricsFromMessage.get(0).getValue(), 0.0001);
    }

    @Test
    public void shouldGetNestedLabelListFromMessage() throws InvalidProtocolBufferException {
        Properties labelNameProtoIndexMapping = new Properties();
        Properties m = new Properties();
        m.setProperty("1", "order_completion_time_seconds");
        labelNameProtoIndexMapping.put("15", m);
        long seconds = System.currentTimeMillis() / 1000;
        TestFeedbackLogMessage feedbackLogMessage =
                TestFeedbackLogMessage.newBuilder().
                        setOrderCompletionTime(Timestamp.newBuilder().setSeconds(seconds)).build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());
        Set<PrometheusLabel> labelsFromMessage = TimeSeriesBuilderUtils.getLabelsFromMessage(dynamicMessage, labelNameProtoIndexMapping, 1);
        Assert.assertEquals(2, labelsFromMessage.size());
        Assert.assertTrue(labelsFromMessage.contains(new PrometheusLabel("order_completion_time_seconds", String.valueOf(seconds))));
        Assert.assertTrue(labelsFromMessage.contains(new PrometheusLabel(PromSinkConstants.KAFKA_PARTITION, "1")));
    }
}
