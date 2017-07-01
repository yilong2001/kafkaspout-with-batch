package com.storm.kafka.consumer.batched;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by yilong on 2017/6/17.
 */
public interface IBatchedTuplesBuilder<K,V> extends Serializable {
    public List<Object> buildBatchedTuple(List<ConsumerRecord<K, V>> records);
}
