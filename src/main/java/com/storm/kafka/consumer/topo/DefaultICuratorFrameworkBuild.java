package com.storm.kafka.consumer.topo;

import com.storm.kafka.consumer.batched.ICuratorFrameworkBuilder;
import com.storm.kafka.consumer.batched.ZkBatchedKafkaSpoutConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.log4j.Logger;

/**
 * Created by yilong on 2017/6/17.
 */
public class DefaultICuratorFrameworkBuild implements ICuratorFrameworkBuilder {
    static Logger LOG = Logger.getLogger(DefaultICuratorFrameworkBuild.class);

    public DefaultICuratorFrameworkBuild() {
    }

    @Override
    public CuratorFramework build(ZkBatchedKafkaSpoutConfig spoutConfig) {
        return CuratorFrameworkFactory.builder()
                .namespace(spoutConfig.getZKRootPath()+"/"+spoutConfig.getConsumerGroupId())
                .connectString(spoutConfig.getZKServer())
                .connectionTimeoutMs(10000)
                .retryPolicy(new RetryNTimes(10, 10000))
                .build();
    }
}
