package com.storm.kafka.consumer.batched;

import org.apache.log4j.Logger;
import org.apache.storm.kafka.spout.KafkaSpoutMessageId;

import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by yilong on 2017/6/12.
 */
public class SpoutAckCollector {
    static Logger LOG = Logger.getLogger(SpoutAckCollector.class);

    //Map<KafkaSpoutMessageId, Boolean> msgAckInfo = new TreeMap<>();
    //Set<KafkaSpoutMessageId> msgAckInfo = new TreeSet();
    private static final Comparator<KafkaSpoutMessageId> OFFSET_COMPARATOR = new OffsetComparator();
    private static class OffsetComparator implements Comparator<KafkaSpoutMessageId> {
        public int compare(KafkaSpoutMessageId m1, KafkaSpoutMessageId m2) {
            return m1.offset() < m2.offset() ? -1 : m1.offset() == m2.offset() ? 0 : 1;
        }
    }

    private NavigableSet<KafkaSpoutMessageId> msgAckInfo = null;

    public SpoutAckCollector() {
        msgAckInfo = new TreeSet<>(OFFSET_COMPARATOR);
    }

    public Set<KafkaSpoutMessageId> getAckInfo() {
        return msgAckInfo;
    }

    public void addMsg(KafkaSpoutMessageId msgid) {
        LOG.info("****** "+Thread.currentThread().getName()+", SpoutAckCollector:add:messageId:"+msgid.toString());
        msgAckInfo.add(msgid);
    }

    public boolean isAcked() {
        if (msgAckInfo.isEmpty()) {
            return true;
        }

        return false;
    }

    public void clear() {
        msgAckInfo.clear();
    }

    public boolean ackedAndClear() {
        if (isAcked()) {
            clear();
            return true;
        }

        return false;
    }

    public boolean containMsgId(KafkaSpoutMessageId msgid) {
        return msgAckInfo.contains(msgid);
    }

    public void ackMsg(KafkaSpoutMessageId msgid) {
        LOG.info("****** "+Thread.currentThread().getName()+", SpoutAckCollector:remove:messageId:"+msgid.toString());
        msgAckInfo.remove(msgid);
    }

    public boolean isEmpty() {
        return msgAckInfo.isEmpty();
    }
}
