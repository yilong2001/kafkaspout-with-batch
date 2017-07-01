package com.storm.kafka.consumer.batched;

import org.apache.curator.framework.CuratorFramework;

/**
 * Created by yilong on 2017/6/17.
 */
public interface ICuratorFrameworkBuilder {
    public CuratorFramework build(ZkBatchedKafkaSpoutConfig spoutConfig);
}
