package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.ProtoIndexToFieldMapConverter;
import org.aeonbits.owner.Config;

import java.util.Properties;

public interface InfluxSinkConfig extends AppConfig {
    @Config.Key("SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING")
    @Config.ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getSinkInfluxFieldNameProtoIndexMapping();

    @Config.Key("SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING")
    @Config.ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getSinkInfluxTagNameProtoIndexMapping();

    @Config.Key("SINK_INFLUX_MEASUREMENT_NAME")
    String getSinkInfluxMeasurementName();

    @Config.Key("SINK_INFLUX_PROTO_EVENT_TIMESTAMP_INDEX")
    Integer getSinkInfluxProtoEventTimestampIndex();

    @Config.Key("SINK_INFLUX_DB_NAME")
    String getSinkInfluxDbName();

    @Config.Key("SINK_INFLUX_RETENTION_POLICY")
    @DefaultValue("autogen")
    String getSinkInfluxRetentionPolicy();

    @Config.Key("SINK_INFLUX_URL")
    String getSinkInfluxUrl();

    @Config.Key("SINK_INFLUX_USERNAME")
    String getSinkInfluxUsername();

    @Config.Key("SINK_INFLUX_PASSWORD")
    String getSinkInfluxPassword();
}

