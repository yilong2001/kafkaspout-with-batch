package com.storm.kafka.consumer.topo;

import com.storm.kafka.consumer.conf.MyTopoConf;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;


/**
 * Created by wangtuo on 17-5-17.
 */
public class BatchedKafkaSpoutTopology {
    static Logger LOG = Logger.getLogger(BatchedKafkaSpoutTopology.class);

    private static String topoName = "batchedKafkaSpoutWithZK";

    public static void main(String[] args) {
        if (args.length > 0) {
            topoName = args[0];
        }

        System.setProperty("user.name", "Guest");

        try {
            MyTopoConf myTopoConf = MyTopoConf.newTopoConf();

            try {
                CuratorFramework cf = CuratorFrameworkFactory.builder()
                        .namespace(myTopoConf.getZkRootPath()+"/"+myTopoConf.getBoltsStreamId())
                        .connectString(myTopoConf.getZkServer())
                        .connectionTimeoutMs(10000)
                        .retryPolicy(new RetryNTimes(10, 10000))
                        .build();
                cf.start();
                Stat stat = cf.checkExists().forPath(myTopoConf.getZkRootPath());
                if (stat == null) {
                    cf.create().withMode(CreateMode.PERSISTENT).withACL(myTopoConf.getACLs()).forPath(myTopoConf.getZkRootPath());
                }

                stat = cf.checkExists().forPath(myTopoConf.getZkRootPath()+"/"+myTopoConf.getBoltsStreamId());
                if (stat == null) {
                    cf.create().withMode(CreateMode.PERSISTENT).withACL(myTopoConf.getACLs()).forPath(myTopoConf.getZkRootPath()+"/"+myTopoConf.getBoltsStreamId());
                }

                cf.close();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                return;
            }

            TopologyBuilder builder = new TopologyBuilder();

            Config conf = myTopoConf.getTopoConf();

            String tops = "topics : ";
            for (String top : myTopoConf.getTopics()) {
                tops += top+",";
            }

            LOG.info(tops);
            //conf.setNumWorkers(2);

            String[] components = {"batchecSpout-1", "boltedBolt"};
            int parallelism = 4; //group.getKafkaSpoutConf().get("parallelism");

            builder.setSpout(components[0], BatchedKafKaSpoutFactory.buildKafkaSpout(myTopoConf), parallelism);

            builder.setBolt(components[1], new BatchedBolt<byte[], byte[]>(myTopoConf.getBoltsStreamId()), 2*parallelism).
                    shuffleGrouping(components[0], myTopoConf.getBoltsStreamId());

            StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
        } catch (Exception e) {
            LOG.error("topology:" + topoName + "submit fiald" + e.getMessage(), e);
        }
    }
}
