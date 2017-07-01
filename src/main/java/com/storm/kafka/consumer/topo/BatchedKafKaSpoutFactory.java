package com.storm.kafka.consumer.topo;

import com.storm.kafka.consumer.batched.BatchedTuplesBuilder;
import com.storm.kafka.consumer.batched.BatchedTuplesBuilderRepo;
import com.storm.kafka.consumer.batched.ZkBatchedKafkaSpout;
import com.storm.kafka.consumer.batched.ZkBatchedKafkaSpoutConfig;
import com.storm.kafka.consumer.conf.MyTopoConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by wangtuo on 17-5-18.
 */
public class BatchedKafKaSpoutFactory {
    static Logger logger = Logger.getLogger(BatchedKafKaSpoutFactory.class);

    public static ZkBatchedKafkaSpout buildKafkaSpout(MyTopoConf myTopoConf) {
        ZkBatchedKafkaSpoutConfig config = getKafkaSpoutConfig(myTopoConf);

        return new ZkBatchedKafkaSpout<byte[], byte[]>(getKafkaSpoutConfig(myTopoConf));
    }

    private static ZkBatchedKafkaSpoutConfig<byte[], byte[]> getKafkaSpoutConfig(MyTopoConf myTopoConf) {
        Map<String, Object> kafkaConsumerProps = myTopoConf.getKafkaConsumerProps();

        KafkaSpoutStreams kafkaSpoutStreams = new KafkaSpoutStreamsNamedTopics.Builder(
                new Fields("key", "value"),
                myTopoConf.getBoltsStreamId(),
                myTopoConf.getTopics().toArray(new String[] {})
        ).build();

        BatchedTuplesBuilderRepo<byte[], byte[]> tuplesBuilderRepo = newTuplesBuilderRepo(myTopoConf);
        KafkaSpoutRetryService retryService = newRetryService();
    
        return new ZkBatchedKafkaSpoutConfig.Builder<byte[], byte[]>(
                kafkaConsumerProps,
                kafkaSpoutStreams,
                tuplesBuilderRepo,
                retryService)
                .setTopicPartitionNum("multihbase10", 4)
                .setZKServer(myTopoConf.getZkServer())
                .setZKRootPath(myTopoConf.getZkRootPath())
                .setZookeeperACLs(myTopoConf.getACLs())
                .setZKConnectClassName(DefaultICuratorFrameworkBuild.class.getCanonicalName())
                .build();
    }

    private static KafkaSpoutRetryService newRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(new TimeInterval(500, TimeUnit.MICROSECONDS),
                TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }

    private static BatchedTuplesBuilderRepo<byte[], byte[]> newTuplesBuilderRepo(MyTopoConf myTopoConf) {
        return new BatchedTuplesBuilderRepo
                .Builder<byte[], byte[]>()
                .addTupleBuild(
                new BatchedTuplesBuilder<byte[], byte[]>(myTopoConf.getTopics().toArray(new String[] {})) {
                    @Override
                    public List<Object> buildBatchedTuple(List<ConsumerRecord<byte[], byte[]>> records) {
                        List<byte[]> keys = new ArrayList<>();
                        List<byte[]> vals = new ArrayList<>();
                        for (ConsumerRecord<byte[], byte[]> rd : records) {
                            keys.add(rd.key());
                            vals.add(rd.value());
                        }

                        return new Values(keys, vals);
                    }
                }
        ).build();
    }
}
