package com.storm.kafka.consumer.batched;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Created by yilong on 2017/6/17.
 */
public class BatchedTuplesBuilderRepo<K,V> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(BatchedTuplesBuilderRepo.class);
    private Map<String, BatchedTuplesBuilder<K, V>> topicToTupleBuilders;

    private BatchedTuplesBuilderRepo(BatchedTuplesBuilderRepo.Builder<K,V> builder) {
        this.topicToTupleBuilders = builder.topicToTupleBuilders;
        LOG.debug("Instantiated {}", this);
    }

    public BatchedTuplesBuilder<K,V> getTupleBuilder(String topic) {
        return topicToTupleBuilders.get(topic);
    }

    public static class Builder<K,V> {
        private List<BatchedTuplesBuilder<K, V>> tupleBuilders;
        private Map<String, BatchedTuplesBuilder<K, V>> topicToTupleBuilders;

        public Builder() {
            this.tupleBuilders = new ArrayList<>();
            topicToTupleBuilders = new HashMap<>();
        }

        public Builder<K,V> addTupleBuild(BatchedTuplesBuilder<K, V> tupleBuilder) {
            this.tupleBuilders.add(tupleBuilder);
            return this;
        }

        public BatchedTuplesBuilderRepo<K,V> build() {
            for (BatchedTuplesBuilder<K, V> tupleBuilder : tupleBuilders) {
                for (String topic : tupleBuilder.getTopics()) {
                    if (!topicToTupleBuilders.containsKey(topic)) {
                        topicToTupleBuilders.put(topic, tupleBuilder);
                    }
                }
            }

            return new BatchedTuplesBuilderRepo(this);
        }
    }
}
