package io.odpf.firehose.sink.redis;


import com.gojek.de.stencil.client.StencilClient;
import io.odpf.firehose.config.RedisSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.sink.redis.client.RedisClient;
import io.odpf.firehose.sink.redis.client.RedisClientFactory;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the RedisSink.
 * <p>
 * The firehose would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient statsDReporter, StencilClient client)}
 * to obtain the RedisSink implementation.
 */
public class RedisSinkFactory implements SinkFactory {

    public AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        RedisSinkConfig redisSinkConfig = ConfigFactory.create(RedisSinkConfig.class, configuration);
        Instrumentation instrumentation = new Instrumentation(statsDReporter, RedisSinkFactory.class);
        String redisConfig = String.format("\n\tredis.urls = %s\n\tredis.key.template = %s\n\tredis.sink.type = %s"
                        + "\n\tredis.list.data.proto.index = %s\n\tredis.ttl.type = %s\n\tredis.ttl.value = %d",
                redisSinkConfig.getSinkRedisUrls(),
                redisSinkConfig.getSinkRedisKeyTemplate(),
                redisSinkConfig.getSinkRedisDataType().toString(),
                redisSinkConfig.getSinkRedisListDataProtoIndex(),
                redisSinkConfig.getSinkRedisTtlType().toString(),
                redisSinkConfig.getSinkRedisTtlValue());
        instrumentation.logDebug(redisConfig);
        instrumentation.logInfo("Redis server type = {}", redisSinkConfig.getSinkRedisDeploymentType());

        RedisClientFactory redisClientFactory = new RedisClientFactory(statsDReporter, redisSinkConfig, stencilClient);
        RedisClient client = redisClientFactory.getClient();
        instrumentation.logInfo("Connection to redis established successfully");
        return new RedisSink(new Instrumentation(statsDReporter, RedisSink.class), "redis", client);
    }
}
