package com.gotocompany.firehose.sink.prometheus.builder;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PrometheusMetric {
    private String name;
    private double value;
}
