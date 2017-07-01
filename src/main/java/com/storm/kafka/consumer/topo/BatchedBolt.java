package com.storm.kafka.consumer.topo;

/**
 * Created by yilong on 2017/5/24.
 */

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by wangtuo on 17-5-15.
 */
public class BatchedBolt<K,V> extends BaseRichBolt {
    static Logger LOG = Logger.getLogger(BatchedBolt.class);

    OutputCollector collector;
    String streamId;

    public BatchedBolt(String streamId) {
        this.streamId = streamId;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void doExecute(K key, V val) {
        //parse and process
        int count=0;
        for(int i=0; i< new Random().nextInt() % 300000; i++) {
            count++;
        }
    }

    @Override
    public void execute(Tuple input) {
        List<K> keys = (List<K>)input.getValueByField("key");
        List<V> values = (List<V>)input.getValueByField("value");

        int okCount=0;
        int failCount=0;
        int cursor = 0;
        for (V val : values) {
            try {
                doExecute(keys.get(cursor), val);
                okCount++;
            } catch (Exception e) {
                failCount++;
                break;
            }
        }

        LOG.info("okCount="+okCount+", and failCount="+failCount);

        if (failCount == 0) {
            this.collector.ack(input);
        } else {
            this.collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(this.streamId, new Fields("key", "value"));
    }
}
