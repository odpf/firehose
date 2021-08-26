package io.odpf.firehose.sink.redis.parsers;


import io.odpf.firehose.config.RedisSinkConfig;
import io.odpf.firehose.config.enums.RedisSinkDataType;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.proto.ProtoToFieldMapper;
import io.odpf.stencil.parser.ProtoParser;

/**
 * Redis parser factory.
 */
public class RedisParserFactory {

    /**
     * Gets parser.
     *
     * @param protoToFieldMapper the proto to field mapper
     * @param protoParser        the proto parser
     * @param redisSinkConfig    the redis sink config
     * @param statsDReporter     the statsd reporter
     * @return RedisParser
     */
    public static RedisParser getParser(ProtoToFieldMapper protoToFieldMapper, ProtoParser protoParser, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {

        RedisParser redisParser;

        if (redisSinkConfig.getSinkRedisDataType().equals(RedisSinkDataType.LIST)) {
            redisParser = new RedisListParser(protoParser, redisSinkConfig, statsDReporter);
        } else {
            redisParser = new RedisHashSetParser(protoToFieldMapper, protoParser, redisSinkConfig, statsDReporter);
        }
        return redisParser;
    }
}
