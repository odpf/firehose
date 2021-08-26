package io.odpf.firehose.sink.redis.parsers;


import io.odpf.firehose.config.RedisSinkConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.proto.ProtoToFieldMapper;
import io.odpf.firehose.sink.redis.dataentry.RedisDataEntry;
import io.odpf.firehose.sink.redis.dataentry.RedisHashSetFieldEntry;
import com.google.protobuf.DynamicMessage;
import io.odpf.stencil.parser.ProtoParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Redis hash set parser.
 */
public class RedisHashSetParser extends RedisParser {
    private ProtoToFieldMapper protoToFieldMapper;
    private RedisSinkConfig redisSinkConfig;
    private StatsDReporter statsDReporter;

    /**
     * Instantiates a new Redis hash set parser.
     *  @param protoToFieldMapper the proto to field mapper
     * @param protoParser        the proto parser
     * @param redisSinkConfig    the redis sink config
     * @param statsDReporter     the statsd reporter
     */
    public RedisHashSetParser(ProtoToFieldMapper protoToFieldMapper, ProtoParser protoParser, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        super(protoParser, redisSinkConfig);
        this.protoToFieldMapper = protoToFieldMapper;
        this.redisSinkConfig = redisSinkConfig;
        this.statsDReporter = statsDReporter;
    }

    @Override
    public List<RedisDataEntry> parse(Message message) {
        DynamicMessage parsedMessage = parseEsbMessage(message);
        String redisKey = parseTemplate(parsedMessage, redisSinkConfig.getSinkRedisKeyTemplate());
        List<RedisDataEntry> messageEntries = new ArrayList<>();
        Map<String, Object> protoToFieldMap = protoToFieldMapper.getFields(getPayload(message));
        protoToFieldMap.forEach((key, value) -> messageEntries.add(new RedisHashSetFieldEntry(redisKey, parseTemplate(parsedMessage, key), String.valueOf(value), new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class))));
        return messageEntries;
    }
}
