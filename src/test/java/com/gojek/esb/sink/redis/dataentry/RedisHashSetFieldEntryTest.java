package com.gojek.esb.sink.redis.dataentry;

import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.redis.ttl.DurationTtl;
import com.gojek.esb.sink.redis.ttl.ExactTimeTtl;
import com.gojek.esb.sink.redis.ttl.NoRedisTtl;
import com.gojek.esb.sink.redis.ttl.RedisTtl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class RedisHashSetFieldEntryTest {
    @Mock
    private Instrumentation instrumentation;

    @Mock
    private Pipeline pipeline;

    @Mock
    private JedisCluster jedisCluster;

    private RedisTtl redisTTL;
    private RedisHashSetFieldEntry redisHashSetFieldEntry;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        redisTTL = new NoRedisTtl();
        redisHashSetFieldEntry = new RedisHashSetFieldEntry("test-key", "test-field", "test-value", instrumentation);
    }

    @Test
    public void shouldIOnlyPushDataWithoutTTLByDefaultForPipeline() {
        redisHashSetFieldEntry.pushMessage(pipeline, redisTTL);

        verify(pipeline, times(1)).hset("test-key", "test-field", "test-value");
        verify(pipeline, times(0)).expireAt(any(String.class), any(Long.class));
        verify(pipeline, times(0)).expireAt(any(String.class), any(Long.class));
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
    }

    @Test
    public void shouldSetProperTTLForExactTimeForPipeline() {
        redisTTL = new ExactTimeTtl(1000L);
        redisHashSetFieldEntry.pushMessage(pipeline, redisTTL);

        verify(pipeline, times(1)).expireAt("test-key", 1000L);
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
    }

    @Test
    public void shouldSetProperTTLForDurationForPipeline() {
        redisTTL = new DurationTtl(1000);
        redisHashSetFieldEntry.pushMessage(pipeline, redisTTL);

        verify(pipeline, times(1)).expire("test-key", 1000);
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
    }

    @Test
    public void shouldIOnlyPushDataWithoutTTLByDefaultForCluster() {
        redisHashSetFieldEntry.pushMessage(jedisCluster, redisTTL);

        verify(jedisCluster, times(1)).hset("test-key", "test-field", "test-value");
        verify(jedisCluster, times(0)).expireAt(any(String.class), any(Long.class));
        verify(jedisCluster, times(0)).expireAt(any(String.class), any(Long.class));
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
    }

    @Test
    public void shouldSetProperTTLForExactTimeForCluster() {
        redisTTL = new ExactTimeTtl(1000L);
        redisHashSetFieldEntry.pushMessage(jedisCluster, redisTTL);

        verify(jedisCluster, times(1)).expireAt("test-key", 1000L);
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
    }

    @Test
    public void shouldSetProperTTLForDuration() {
        redisTTL = new DurationTtl(1000);
        redisHashSetFieldEntry.pushMessage(jedisCluster, redisTTL);

        verify(jedisCluster, times(1)).expire("test-key", 1000);
        verify(instrumentation, times(1)).logDebug("key: {}, field: {}, value: {}", "test-key", "test-field", "test-value");
    }
}
