package com.gotocompany.firehose.sink.prometheus.builder;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PrometheusLabel {
    private String name;
    private String value;
}
