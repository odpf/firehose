package io.odpf.firehose.sink.redis.parsers;


import io.odpf.firehose.config.RedisSinkConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.redis.dataentry.RedisDataEntry;
import io.odpf.firehose.sink.redis.dataentry.RedisListEntry;
import com.google.protobuf.DynamicMessage;
import io.odpf.stencil.parser.ProtoParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis list parser.
 */
public class RedisListParser extends RedisParser {
    private RedisSinkConfig redisSinkConfig;
    private StatsDReporter statsDReporter;

    /**
     * Instantiates a new Redis list parser.
     *
     * @param protoParser     the proto parser
     * @param redisSinkConfig the redis sink config
     * @param statsDReporter  the stats d reporter
     */
    public RedisListParser(ProtoParser protoParser, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        super(protoParser, redisSinkConfig);
        this.redisSinkConfig = redisSinkConfig;
        this.statsDReporter = statsDReporter;
    }

    @Override
    public List<RedisDataEntry> parse(Message message) {
        DynamicMessage parsedMessage = parseEsbMessage(message);
        String redisKey = parseTemplate(parsedMessage, redisSinkConfig.getSinkRedisKeyTemplate());
        String protoIndex = redisSinkConfig.getSinkRedisListDataProtoIndex();
        if (protoIndex == null) {
            throw new IllegalArgumentException("Please provide SINK_REDIS_LIST_DATA_PROTO_INDEX in list sink");
        }
        List<RedisDataEntry> messageEntries = new ArrayList<>();
        messageEntries.add(new RedisListEntry(redisKey, getDataByFieldNumber(parsedMessage, protoIndex).toString(), new Instrumentation(statsDReporter, RedisListEntry.class)));
        return messageEntries;
    }
}
