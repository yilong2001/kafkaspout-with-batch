package com.storm.kafka.consumer.batched;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * Created by yilong on 2017/6/1.
 */
public class ZkBatchedKafkaSpout<K, V> extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(ZkBatchedKafkaSpout.class);

    // Kafka
    private final ZkBatchedKafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private transient KafkaConsumer<K, V> kafkaConsumer;
    private transient KafkaSpoutTuplesBuilder<List<byte[]>, List<byte[]>> tuplesBuilder;

    private KafkaSpoutStreams kafkaSpoutStreams;
    private transient BatchedTuplesBuilderRepo tuplesBuilderRepo;

    //private transient String zkServers = null;
    //private transient ZooKeeper zooKeeper = null;
    private transient CuratorFramework curatorFramework = null;

    List<ZkTopicPartitionAssigner> assigners = new ArrayList<>();

    int maxRetries;

    private ZkBatchedKafkaSpoutProcessor ZkBatchedKafkaSpoutProcessor;

    public ZkBatchedKafkaSpout(ZkBatchedKafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;                 // Pass in configuration
        this.kafkaSpoutStreams = kafkaSpoutConfig.getKafkaSpoutStreams();
        this.tuplesBuilderRepo = kafkaSpoutConfig.getTuplesBuilderRepo();
    }

    public List<String> getTopics(KafkaSpoutStreams spoutStreams) {
        if (spoutStreams instanceof KafkaSpoutStreamsNamedTopics) {
            final List<String> topics = ((KafkaSpoutStreamsNamedTopics) spoutStreams).getTopics();
            LOG.info("Kafka consumer subscribed topics {}", topics);
            return topics;
        }

        LOG.error("only support KafkaSpoutStreamsNamedTopics!");

        return null;
    }

    private void createAssigner(String topic) {
        ZkTopicPartitionPath path = new ZkTopicPartitionPath(kafkaSpoutConfig.getZKRootPath(),
                kafkaSpoutConfig.getConsumerGroupId(),
                topic);

        SimpleTimer timer = new SimpleTimer(500, kafkaSpoutConfig.getMinBackoffTimeoutMs(), TimeUnit.MILLISECONDS);

        int maxPartitions = kafkaSpoutConfig.getTopicPartitionNum(topic);

        ZkTopicPartitionAssigner assigner = new ZkTopicPartitionAssigner(curatorFramework,
                path,
                maxPartitions,
                timer,
                kafkaSpoutConfig.getZookeeperACLs());

        assigners.add(assigner);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        try {
            Class zkCls = Class.forName(kafkaSpoutConfig.getZKConnectClassName());
            Object obj = zkCls.newInstance();
            if (obj instanceof ICuratorFrameworkBuilder) {
                curatorFramework = ((ICuratorFrameworkBuilder)obj).build(kafkaSpoutConfig);
                curatorFramework.start();
            } else {
                throw new RuntimeException(kafkaSpoutConfig.getZKConnectClassName()+" error!");
            }
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        List<String> topics = getTopics(this.kafkaSpoutStreams);
        if (topics == null) {
            throw new RuntimeException("topics is null...");
        }

        for (String topic : topics) {
            createAssigner(topic);
        }

        maxRetries = kafkaSpoutConfig.getMaxTupleRetries();

        ZkBatchedKafkaSpoutProcessor = new ZkBatchedKafkaSpoutProcessor(collector,
                kafkaSpoutStreams,
                tuplesBuilderRepo,
                kafkaSpoutConfig,
                assigners,
                curatorFramework);

        // Offset management
        //firstPollOffsetStrategy = kafkaSpoutConfig.getFirstPollOffsetStrategy();

        // Retries management
        //retryService = kafkaSpoutConfig.getRetryService();

        // Tuples builder delegate
        //tuplesBuilder = kafkaSpoutConfig.getTuplesBuilder();

        //if (!consumerAutoCommitMode) {     // If it is auto commit, no need to commit offsets manually
        //    commitTimer = new ZkBatchedKafkaSpout.Timer(500, kafkaSpoutConfig.getOffsetsCommitPeriodMs(), TimeUnit.MILLISECONDS);
        //}

        //acked = new HashMap<>();
        //emitted = new HashSet<>();
        //waitingToEmit = Collections.emptyListIterator();

        LOG.info("Kafka Spout opened with the following configuration: {}", kafkaSpoutConfig);
    }

    // ======== Next Tuple =======
    @Override
    public void nextTuple() {
        //allocate partition for topic that has been not been allocated
        ZkBatchedKafkaSpoutProcessor.doPartitionsAssign();

        //processs topic-partition with all msg batchs have been acked
        ZkBatchedKafkaSpoutProcessor.doCommit();

        ZkBatchedKafkaSpoutProcessor.doPoll();

        //获取topic partition的下一个offset
        ZkBatchedKafkaSpoutProcessor.doEmit();

        LOG.info(" nextTuple still running! ");
    }

   // ======== Ack =======
    @Override
    public void ack(Object messageId) {
        ZkBatchedKafkaSpoutProcessor.onAck(messageId);
    }

    // ======== Fail =======

    @Override
    public void fail(Object messageId) {
        ZkBatchedKafkaSpoutProcessor.onFail(messageId);
    }

    // ======== Activate / Deactivate / Close / Declare Outputs =======
    @Override
    public void activate() {
        LOG.info("********* activate ***********8");
        subscribeKafkaConsumer();
    }

    private void subscribeKafkaConsumer() {
        kafkaConsumer = new KafkaConsumer<>(kafkaSpoutConfig.getKafkaProps(),
                kafkaSpoutConfig.getKeyDeserializer(), kafkaSpoutConfig.getValueDeserializer());

        kafkaConsumer.assign(Arrays.asList(new TopicPartition("multihbase05", 0)));

        ZkBatchedKafkaSpoutProcessor.setKafkaConsumer(kafkaConsumer);
    }

    @Override
    public void deactivate() {
        shutdown();
    }

    @Override
    public void close() {
        shutdown();
    }

    private void shutdown() {
        try {
            kafkaConsumer.wakeup();
            ZkBatchedKafkaSpoutProcessor = null;
        } finally {
            //remove resources
            kafkaConsumer.close();
            try {
                curatorFramework.close();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        kafkaSpoutStreams.declareOutputFields(declarer);
    }

    @Override
    public String toString() {
        return ZkBatchedKafkaSpoutProcessor.toString();
    }

    @Override
    public Map<String, Object> getComponentConfiguration () {
        Map<String, Object> configuration = super.getComponentConfiguration();
        if (configuration == null) {
            configuration = new HashMap<>();
        }
        String configKeyPrefix = "config.";

        if (kafkaSpoutStreams instanceof KafkaSpoutStreamsNamedTopics) {
            configuration.put(configKeyPrefix + "topics", getNamedTopics());
        } else if (kafkaSpoutStreams instanceof KafkaSpoutStreamsWildcardTopics) {
            configuration.put(configKeyPrefix + "topics", getWildCardTopics());
        }

        configuration.put(configKeyPrefix + "groupid", kafkaSpoutConfig.getConsumerGroupId());
        configuration.put(configKeyPrefix + "bootstrap.servers", kafkaSpoutConfig.getKafkaProps().get("bootstrap.servers"));
        return configuration;
    }

    private String getNamedTopics() {
        StringBuilder topics = new StringBuilder();
        for (String topic: kafkaSpoutConfig.getSubscribedTopics()) {
            topics.append(topic).append(",");
        }
        return topics.toString();
    }

    private String getWildCardTopics() {
        return kafkaSpoutConfig.getTopicWildcardPattern().toString();
    }

    
}
