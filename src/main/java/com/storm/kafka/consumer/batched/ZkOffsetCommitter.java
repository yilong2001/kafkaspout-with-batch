package com.storm.kafka.consumer.batched;

import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * Created by yilong on 2017/6/12.
 */
public class ZkOffsetCommitter {
    static Logger LOG = Logger.getLogger(ZkOffsetCommitter.class);

    CuratorFramework curatorFramework = null;
    ZkTopicPartitionPath path = null;
    int partition = 0;
    List<ACL> acls;

    long nextCommitOffset = -1;

    public ZkOffsetCommitter(CuratorFramework curatorFramework,
                             ZkTopicPartitionPath zkTopicPartitionPath,
                             int partition,
                             List<ACL> acls) {
        this.curatorFramework = curatorFramework;
        this.path = zkTopicPartitionPath;
        this.partition = partition;
        this.acls = acls;
        this.nextCommitOffset = -1;
    }

    public long getFetchOffset() {
        if (this.nextCommitOffset >= 0) {
            return this.nextCommitOffset;
        }

        long offset = 0;
        try {
            Stat stat = null;
            try {
                stat = curatorFramework.checkExists().forPath(path.getPersistPath(partition));
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }

            if (stat == null) {
                String res = null;
                try {
                    res = curatorFramework
                            .create()
                            .withMode(CreateMode.PERSISTENT)
                            .withACL(acls)
                            .forPath(path.getPersistPath(partition), (""+offset).getBytes());
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }

                try {
                    stat = curatorFramework.setData().forPath(path.getPersistPath(partition), (""+offset).getBytes());
                    res = stat.toString();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }

                if (res == null) {
                    LOG.error("****** "+Thread.currentThread().getName()+": cannot getPersistNodeData From zookeeper");
                    return -1;
                }

                LOG.info("****** "+Thread.currentThread().getName()+": firstly initial offset : "+offset);
                nextCommitOffset = offset;
                return nextCommitOffset;
            } else {
                try {
                    byte[] out = curatorFramework.getData().forPath(path.getPersistPath(partition));
                    try {
                        nextCommitOffset = Long.parseLong(new String(out));
                        LOG.info("****** "+Thread.currentThread().getName()+": successfully fetch "+path.getPersistPath(partition)+", commit offset : "+nextCommitOffset);
                        return nextCommitOffset;
                    } catch (NumberFormatException e) {
                        LOG.error(e.getMessage(), e);
                    }
                } catch (KeeperException e) {
                    LOG.error(e.getMessage(), e);
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }

        return -1;
    }

    public boolean commitOffset(long offset) {
        if (offset < 0) {
            LOG.error("****** "+Thread.currentThread().getName()+": the commited offset is lower than 0...");
            return false;
        }

        long next = offset + 1;
        try {
            Stat stat = curatorFramework.setData().forPath(path.getPersistPath(partition), (""+next).getBytes());
            if (stat != null) {
                LOG.info("****** "+Thread.currentThread().getName()+": commit nextOffset done : "+next);
                this.nextCommitOffset = next;
                return true;
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        return false;
    }

    public String getTopic() {
        return path.getTopic();
    }
}
