package com.storm.kafka.consumer.batched;

import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Random;

/**
 * Created by yilong on 2017/6/12.
 */
public class ZkTopicPartitionAssigner {
    static Logger LOG = Logger.getLogger(ZkTopicPartitionAssigner.class);

    CuratorFramework curatorFramework = null;
    int partition = -1;
    int maxPartitions = 0;
    String parentPath;
    SimpleTimer timer;
    Random random;

    public static final int MaxBackOffPerMs = 20000;
    public static final int MinBackOffPerMs = 2000;

    List<ACL> acls;
    ZkTopicPartitionPath zkTopicPartitionPath;

    public ZkTopicPartitionAssigner(CuratorFramework curatorFramework,
                                    ZkTopicPartitionPath zkTopicPartitionPath,
                                    int maxPartitions,
                                    SimpleTimer timer,
                                    List<ACL> acls) {
        this.acls = acls;
        this.curatorFramework = curatorFramework;
        this.maxPartitions = maxPartitions;
        this.zkTopicPartitionPath = zkTopicPartitionPath;
        this.timer = timer;
        this.random = new Random();
    }

    public int assignPartiton() {
        if (this.partition >= 0) {
            return this.partition;
        }

        if (!timer.isExpiredResetOnTrue()) {
            LOG.info("assignPartiton backoffTimer is not timeout...");
            return this.partition;
        }

        //Through zookeeper to create ephemeral node to hold the partition
        for (int curPartiton=0; curPartiton<maxPartitions; curPartiton++) {
            String res = null;
            //if EPHEMERAL node is exists, should try next partition
            Stat stat = null;
            try {
                stat = curatorFramework.checkExists().forPath(zkTopicPartitionPath.getEphemeralPath(curPartiton));
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }

            if (stat == null) {
                try {
                    res = curatorFramework
                            .create()
                            .withMode(CreateMode.EPHEMERAL)
                            .withACL(acls)
                            .forPath(zkTopicPartitionPath.getEphemeralPath(curPartiton),
                            "0".getBytes());
                } catch (Exception e) {
                    LOG.info(e.getMessage(), e);
                }
            } else {
                //LOG.info("****** zookeeper node is exist : " + stat.toString());
            }

            if (res != null) {
                this.partition = curPartiton;
                return this.partition;
            }
        }

        int period = (random.nextInt() % (MaxBackOffPerMs - MinBackOffPerMs)) + MinBackOffPerMs;
        timer.reStart(period);

        return -1;
    }

    public void cleanPartition() {
        this.partition = -1;
    }

    public int getPartition() {
        return this.partition;
    }

    final public String getTopic() {
        return zkTopicPartitionPath.getTopic();
    }

    final public ZkTopicPartitionPath getZkTopicPartitionPath() {
        return zkTopicPartitionPath;
    }
}
