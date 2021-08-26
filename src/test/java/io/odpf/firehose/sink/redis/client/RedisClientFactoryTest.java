package io.odpf.firehose.sink.redis.client;


import io.odpf.firehose.config.RedisSinkConfig;
import io.odpf.firehose.config.enums.RedisSinkDeploymentType;
import io.odpf.firehose.config.enums.RedisSinkDataType;
import io.odpf.firehose.config.enums.RedisSinkTtlType;
import io.odpf.firehose.exception.EglcConfigurationException;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.stencil.client.StencilClient;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisClientFactoryTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private RedisSinkConfig redisSinkConfig;

    @Mock
    private StencilClient stencilClient;

    @Mock
    private StatsDReporter statsDReporter;

    @Test
    public void shouldGetStandaloneClient() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.LIST);
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.STANDALONE);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn("0.0.0.0:0");

        RedisClientFactory redisClientFactory = new RedisClientFactory(statsDReporter, redisSinkConfig, stencilClient);

        RedisClient client = redisClientFactory.getClient();

        Assert.assertEquals(RedisStandaloneClient.class, client.getClass());
    }

    @Test
    public void shouldGetStandaloneClientWhenURLHasSpaces() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.LIST);
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.STANDALONE);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn(" 0.0.0.0:0 ");
        RedisClientFactory redisClientFactory = new RedisClientFactory(statsDReporter, redisSinkConfig, stencilClient);

        RedisClient client = redisClientFactory.getClient();

        Assert.assertEquals(RedisStandaloneClient.class, client.getClass());
    }

    @Test
    public void shouldGetClusterClient() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.LIST);
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.CLUSTER);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn("0.0.0.0:0, 1.1.1.1:1");
        RedisClientFactory redisClientFactory = new RedisClientFactory(statsDReporter, redisSinkConfig, stencilClient);

        RedisClient client = redisClientFactory.getClient();

        Assert.assertEquals(RedisClusterClient.class, client.getClass());
    }

    @Test
    public void shouldGetClusterClientWhenURLHasSpaces() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.LIST);
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.CLUSTER);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn(" 0.0.0.0:0, 1.1.1.1:1 ");
        RedisClientFactory redisClientFactory = new RedisClientFactory(statsDReporter, redisSinkConfig, stencilClient);

        RedisClient client = redisClientFactory.getClient();

        Assert.assertEquals(RedisClusterClient.class, client.getClass());
    }

    @Test
    public void shouldThrowExceptionWhenUrlIsInvalidForCluster() {
        expectedException.expect(EglcConfigurationException.class);
        expectedException.expectMessage("Invalid url(s) for redis cluster: localhost:6379,localhost:6378,localhost");

        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.LIST);
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.CLUSTER);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn("localhost:6379,localhost:6378,localhost");

        RedisClientFactory redisClientFactory = new RedisClientFactory(statsDReporter, redisSinkConfig, stencilClient);

        redisClientFactory.getClient();
    }

    @Test
    public void shouldThrowExceptionWhenUrlIsInvalidForStandalone() {
        expectedException.expect(EglcConfigurationException.class);
        expectedException.expectMessage("Invalid url for redis standalone: localhost");

        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.LIST);
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisDeploymentType()).thenReturn(RedisSinkDeploymentType.STANDALONE);
        when(redisSinkConfig.getSinkRedisUrls()).thenReturn("localhost");

        RedisClientFactory redisClientFactory = new RedisClientFactory(statsDReporter, redisSinkConfig, stencilClient);

        redisClientFactory.getClient();
    }
}
