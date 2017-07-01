package com.storm.kafka.consumer.batched;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.storm.kafka.spout.*;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by yilong on 2017/6/17.
 */
public class ZkBatchedKafkaSpoutConfig<K,V> implements Serializable {
    public static final int DEFAULT_MAX_RETRIES = Integer.MAX_VALUE;     // Retry forever

    public static final int DEFAULT_MAX_POLL_TIMEOUT_MS = 500;
    public static final int DEFAULT_POLL_TIMEOUT_MS_ONCE = 100;
    public static final int DEFAULT_MAX_RECORDS_PER_BATCH = 10000;
    public static final int DEFAULT_MAX_BLOCKS_PER_BATCH = 4;
    public static final int DEFAULT_MIN_RECORDS_PER_BLOCK = 3200;
    public static final int DEFAULT_MIN_BACKOFF_TIMEOUT_MS = 1000;

    private static final String ACL_SERDE_TAG = "acls";

    // Kafka property names
    public interface Consumer {
        String GROUP_ID = "group.id";
        String BOOTSTRAP_SERVERS = "bootstrap.servers";
        String ENABLE_AUTO_COMMIT = "enable.auto.commit";
        String AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";
        String KEY_DESERIALIZER = "key.deserializer";
        String VALUE_DESERIALIZER = "value.deserializer";
    }

    public enum FirstPollOffsetStrategy {
        EARLIEST,
        LATEST}

    // Kafka consumer configuration
    private final Map<String, Object> kafkaProps;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    private final int maxRecordsPerBatch;
    private final int maxPollTimeoutMsPerBatch;
    private final int pollDurationMsPerOnce;
    private final int maxBlocksPerBatch;
    private final int minRecordsPerBlock;

    private final int minBackoffTimeoutMs;

    private final String zkServer;
    private final String zkRootPath;
    private final String zkConnectClassName;
    private final byte[] aclListBytes;

    // Kafka spout configuration
    private final int maxRetries;
    private final ZkBatchedKafkaSpoutConfig.FirstPollOffsetStrategy firstPollOffsetStrategy;
    private final KafkaSpoutStreams kafkaSpoutStreams;

    private final BatchedTuplesBuilderRepo<K, V> tuplesBuilderRepo;
    private final KafkaSpoutRetryService retryService;

    private ZkBatchedKafkaSpoutConfig(ZkBatchedKafkaSpoutConfig.Builder<K,V> builder) {
        this.kafkaProps = setDefaultsAndGetKafkaProps(builder.kafkaProps);
        this.keyDeserializer = builder.keyDeserializer;
        this.valueDeserializer = builder.valueDeserializer;

        this.maxRetries = builder.maxRetries;
        this.firstPollOffsetStrategy = builder.firstPollOffsetStrategy;
        this.kafkaSpoutStreams = builder.kafkaSpoutStreams;

        this.tuplesBuilderRepo = builder.tuplesBuilderRepo;
        this.retryService = builder.retryService;

        this.maxBlocksPerBatch = builder.maxBlocksPerBatch;
        this.maxPollTimeoutMsPerBatch = builder.maxPollTimeoutMsPerBatch;
        this.pollDurationMsPerOnce = builder.pollDurationMsPerOnce;
        this.maxRecordsPerBatch = builder.maxRecordsPerBatch;
        this.minRecordsPerBlock = builder.minRecordsPerBlock;

        this.minBackoffTimeoutMs = builder.minBackoffTimeoutMs;

        this.zkRootPath = builder.zkRootPath;

        this.zkServer = builder.zkServer;
        this.zkConnectClassName = builder.zkConnectClassName;
        this.aclListBytes = builder.aclListBytes;
    }

    private Map<String, Object> setDefaultsAndGetKafkaProps(Map<String, Object> kafkaProps) {
        // set defaults for properties not specified
        kafkaProps.put(ZkBatchedKafkaSpoutConfig.Consumer.ENABLE_AUTO_COMMIT, "false");
        return kafkaProps;
    }

    public static class Builder<K,V> {
        private final Map<String, Object> kafkaProps;
        private Deserializer<K> keyDeserializer;
        private Deserializer<V> valueDeserializer;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private ZkBatchedKafkaSpoutConfig.FirstPollOffsetStrategy firstPollOffsetStrategy = FirstPollOffsetStrategy.EARLIEST;
        private final KafkaSpoutStreams kafkaSpoutStreams;

        private final BatchedTuplesBuilderRepo<K, V> tuplesBuilderRepo;

        private final KafkaSpoutRetryService retryService;

        private int maxPollTimeoutMsPerBatch = DEFAULT_MAX_POLL_TIMEOUT_MS;
        private int pollDurationMsPerOnce = DEFAULT_POLL_TIMEOUT_MS_ONCE;
        private int maxRecordsPerBatch = DEFAULT_MAX_RECORDS_PER_BATCH;
        private int maxBlocksPerBatch = DEFAULT_MAX_BLOCKS_PER_BATCH;
        private int minRecordsPerBlock = DEFAULT_MIN_RECORDS_PER_BLOCK;

        private int minBackoffTimeoutMs = DEFAULT_MIN_BACKOFF_TIMEOUT_MS;

        private String zkRootPath;
        private String zkServer;

        private String zkConnectClassName;
        private byte[] aclListBytes;

        public Builder(Map<String, Object> kafkaProps, KafkaSpoutStreams kafkaSpoutStreams,
                       BatchedTuplesBuilderRepo<K,V> tuplesBuilderRepo) {
            this(kafkaProps,
                    kafkaSpoutStreams,
                    tuplesBuilderRepo,
                    new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0),
                            KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2),
                            DEFAULT_MAX_RETRIES,
                            KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10)));
        }

        public Builder(Map<String, Object> kafkaProps,
                       KafkaSpoutStreams kafkaSpoutStreams,
                       BatchedTuplesBuilderRepo<K,V> tuplesBuilderRepo,
                       KafkaSpoutRetryService retryService) {
            if (kafkaProps == null || kafkaProps.isEmpty()) {
                throw new IllegalArgumentException("Properties defining consumer connection to Kafka broker are required: " + kafkaProps);
            }

            if (kafkaSpoutStreams == null)  {
                throw new IllegalArgumentException("Must specify stream associated with each topic. Multiple topics can emit to the same stream");
            }

            if (tuplesBuilderRepo == null) {
                throw new IllegalArgumentException("Must specify at last one tuple builder per topic declared in KafkaSpoutStreams");
            }

            if (retryService == null) {
                throw new IllegalArgumentException("Must specify at implementation of retry service");
            }

            this.kafkaProps = kafkaProps;
            this.kafkaSpoutStreams = kafkaSpoutStreams;
            this.tuplesBuilderRepo = tuplesBuilderRepo;
            this.retryService = retryService;
        }

        /**
         * Specifying this key deserializer overrides the property key.deserializer
         */
        public ZkBatchedKafkaSpoutConfig.Builder<K,V> setKeyDeserializer(Deserializer<K> keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
            return this;
        }

        /**
         * Specifying this value deserializer overrides the property value.deserializer
         */
        public ZkBatchedKafkaSpoutConfig.Builder<K,V> setValueDeserializer(Deserializer<V> valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
            return this;
        }

        public ZkBatchedKafkaSpoutConfig.Builder<K,V> setMaxPollTimeoutMsPerBatch(int maxPollTimeoutMsPerBatch) {
            this.maxPollTimeoutMsPerBatch = maxPollTimeoutMsPerBatch;
            return this;
        }

        public ZkBatchedKafkaSpoutConfig.Builder<K,V> setPollDurationMsPerOnce(int pollDurationMsPerOnce) {
            this.pollDurationMsPerOnce = pollDurationMsPerOnce;
            return this;
        }

        public ZkBatchedKafkaSpoutConfig.Builder<K,V> setMaxRecordsPerBatch(int maxRecordsPerBatch) {
            this.maxRecordsPerBatch = maxRecordsPerBatch;
            return this;
        }

        public ZkBatchedKafkaSpoutConfig.Builder<K,V> setMaxBlocksPerBatch(int maxBlocksPerBatch) {
            this.maxBlocksPerBatch = maxBlocksPerBatch;
            return this;
        }

        public ZkBatchedKafkaSpoutConfig.Builder<K,V> setMinRecordsPerBlock(int minRecordsPerBlock) {
            this.minRecordsPerBlock = minRecordsPerBlock;
            return this;
        }

        public ZkBatchedKafkaSpoutConfig.Builder<K,V> setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public ZkBatchedKafkaSpoutConfig.Builder<K, V> setFirstPollOffsetStrategy(ZkBatchedKafkaSpoutConfig.FirstPollOffsetStrategy firstPollOffsetStrategy) {
            this.firstPollOffsetStrategy = firstPollOffsetStrategy;
            return this;
        }

        public ZkBatchedKafkaSpoutConfig.Builder<K, V> setTopicPartitionNum(String topic, int partitionNum) {
            this.kafkaProps.put(topic+".partition.num", partitionNum);
            return this;
        }

        public ZkBatchedKafkaSpoutConfig.Builder<K, V> setMinBackoffTimeoutMs(int minBackoffTimeoutMs) {
            this.minBackoffTimeoutMs = minBackoffTimeoutMs;
            return this;
        }

        public ZkBatchedKafkaSpoutConfig.Builder<K, V> setZKRootPath(String zkRootPath) {
            this.zkRootPath = zkRootPath;
            return this;
        }

        public ZkBatchedKafkaSpoutConfig.Builder<K, V> setZKServer(String zkServer) {
            this.zkServer = zkServer;
            return this;
        }

        public ZkBatchedKafkaSpoutConfig.Builder<K, V> setZKConnectClassName(String zkConnectClassName) {
            this.zkConnectClassName = zkConnectClassName;
            return this;
        }

        public ZkBatchedKafkaSpoutConfig.Builder<K, V> setZookeeperACLs(List<ACL> acls) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
            try {
                boa.writeInt(acls.size(), ACL_SERDE_TAG);
                for (int i=0; i<acls.size(); i++) {
                    boa.writeRecord(acls.get(i), ACL_SERDE_TAG);
                }
                aclListBytes = baos.toByteArray();
            } catch (IOException e) {
                throw new IllegalArgumentException("Zookeeper ACLS are wrong", e);
            }
            return this;
        }

        public ZkBatchedKafkaSpoutConfig<K,V> build() {
            return new ZkBatchedKafkaSpoutConfig<>(this);
        }
    }

    public Map<String, Object> getKafkaProps() {
        return kafkaProps;
    }

    public Deserializer<K> getKeyDeserializer() {
        return keyDeserializer;
    }

    public Deserializer<V> getValueDeserializer() {
        return valueDeserializer;
    }

    public int getMaxPollTimeoutMsPerBatch() {
        return maxPollTimeoutMsPerBatch;
    }

    public int getPollDurationMsPerOnce() {
        return pollDurationMsPerOnce;
    }

    public int getMaxRecordsPerBatch() {
        return maxRecordsPerBatch;
    }

    public int getMaxBlocksPerBatch() {
        return maxBlocksPerBatch;
    }

    public int getMinRecordsPerBlock() {
        return minRecordsPerBlock;
    }

    public String getConsumerGroupId() {
        return (String) kafkaProps.get(ZkBatchedKafkaSpoutConfig.Consumer.GROUP_ID);
    }

    public List<String> getSubscribedTopics() {
        return kafkaSpoutStreams instanceof KafkaSpoutStreamsNamedTopics ?
                new ArrayList<>(((KafkaSpoutStreamsNamedTopics) kafkaSpoutStreams).getTopics()) :
                null;
    }

    public Pattern getTopicWildcardPattern() {
        return kafkaSpoutStreams instanceof KafkaSpoutStreamsWildcardTopics ?
                ((KafkaSpoutStreamsWildcardTopics)kafkaSpoutStreams).getTopicWildcardPattern() :
                null;
    }

    public int getMaxTupleRetries() {
        return maxRetries;
    }

    public int getMinBackoffTimeoutMs() {
        return minBackoffTimeoutMs;
    }

    public String getZKRootPath() {
        return zkRootPath;
    }

    public String getZKServer() {
        return zkServer;
    }

    public String getZKConnectClassName() {
        return zkConnectClassName;
    }

    public List<ACL> getZookeeperACLs() {
        List<ACL> acls = new ArrayList<>();
        ByteArrayInputStream bais = new ByteArrayInputStream(aclListBytes);
        BinaryInputArchive bia = BinaryInputArchive.getArchive(bais);
        try {
            int size = bia.readInt(ACL_SERDE_TAG);
            for (int i=0; i<size; i++) {
                ACL acl = new ACL();
                bia.readRecord(acl, ACL_SERDE_TAG);
                acls.add(acl);
            }
        } catch (IOException e) {
            acls.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE));
        }

        return acls;
    }

    public ZkBatchedKafkaSpoutConfig.FirstPollOffsetStrategy getFirstPollOffsetStrategy() {
        return firstPollOffsetStrategy;
    }

    public KafkaSpoutStreams getKafkaSpoutStreams() {
        return kafkaSpoutStreams;
    }

    public BatchedTuplesBuilderRepo<K, V> getTuplesBuilderRepo() {
        return tuplesBuilderRepo;
    }

    public KafkaSpoutRetryService getRetryService() {
        return retryService;
    }

    public int getTopicPartitionNum(String topic) {
        if (kafkaProps.containsKey(topic+".partition.num")) {
            return (int)kafkaProps.get(topic+".partition.num");
        }

        return 4;
    }

    @Override
    public String toString() {
        return "ZkBatchedKafkaSpoutConfig{" +
                "kafkaProps=" + kafkaProps +
                ", keyDeserializer=" + keyDeserializer +
                ", valueDeserializer=" + valueDeserializer +
                ", maxPollTimeoutMsPerBatch=" + maxPollTimeoutMsPerBatch +
                ", pollDurationMsPerOnce=" + pollDurationMsPerOnce +
                ", maxRecordsPerBatch=" + maxRecordsPerBatch +
                ", maxBlocksPerBatch=" + maxBlocksPerBatch +
                ", minRecordsPerBlock=" + minRecordsPerBlock +
                ", maxRetries=" + maxRetries +
                ", firstPollOffsetStrategy=" + firstPollOffsetStrategy +
                ", kafkaSpoutStreams=" + kafkaSpoutStreams +
                ", tuplesBuilderRepo=" + tuplesBuilderRepo +
                ", retryService=" + retryService +
                ", topics=" + getSubscribedTopics() +
                ", topicWildcardPattern=" + getTopicWildcardPattern() +
                '}';
    }
}
