package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

public interface SinkPoolConfig extends AppConfig {
    @Config.Key("SINK_POOL_NUM_THREADS")
    @Config.DefaultValue("1")
    int getSinkPoolNumThreads();

    @Config.Key("SINK_POOL_QUEUE_POLL_TIMEOUT_MS")
    @Config.DefaultValue("1000")
    int getSinkPoolQueuePollTimeoutMS();
}
