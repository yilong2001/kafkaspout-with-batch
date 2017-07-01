package com.storm.kafka.consumer.batched;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.*;

/**
 * Created by yilong on 2017/6/17.
 */
public abstract class BatchedTuplesBuilder<K,V> implements IBatchedTuplesBuilder<K,V> {
    private List<String> topics;

    public BatchedTuplesBuilder(String... topics) {
        if (topics == null || topics.length == 0) {
            throw new IllegalArgumentException("Must specify at least one topic. It cannot be null or empty");
        }
        this.topics = Arrays.asList(topics);
    }

    public List<String> getTopics() {
        return Collections.unmodifiableList(topics);
    }

    @Override
    public abstract List<Object> buildBatchedTuple(List<ConsumerRecord<K, V>> records);
}
