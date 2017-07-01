package com.storm.kafka.consumer.batched;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.storm.kafka.spout.KafkaSpoutMessageId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yilong on 2017/6/16.
 */
public class TopicPartitionDataRepo<K, V>  {
    static Logger LOG = Logger.getLogger(TopicPartitionDataRepo.class);

    static final int MaxRecordsPerBatch = 10000;
    static final int MaxPollDurationMs = 500;
    static final int PollDurationMsPerOnce = 100;
    static final int DefaultMaxBlocks = 4;
    static final int DefaultMinRecordsPerBlock = 3000;


    private int maxBlocks = DefaultMaxBlocks;
    private int minRecordsPerBlock = DefaultMinRecordsPerBlock;
    private long nextCommitOffset = -1;

    private List<KafkaSpoutMessageId> retryMessageList;
    private BatchStatus status;

    private SpoutAckCollector spoutAckCollector;
    private List<List<ConsumerRecord<K,V>>> emittedMsgs;
    private ZkOffsetCommitter offsetOperator;

    public TopicPartitionDataRepo(ZkOffsetCommitter operator) {
        this.spoutAckCollector = new SpoutAckCollector();
        this.emittedMsgs = new ArrayList<>();
        this.offsetOperator = operator;

        this.retryMessageList = new ArrayList<>();
        this.status = BatchStatus.Init;
    }

    public BatchStatus getStatus() {
        return status;
    }

    public void setStatus(BatchStatus st) {
        this.status = st;
    }

    public void addRecords(List<ConsumerRecord<K,V>> records) {
        int actualBlocks = maxBlocks;
        int recordsPerBlock = minRecordsPerBlock;
        if (records.size() < maxBlocks * minRecordsPerBlock) {
            actualBlocks = (records.size()) / minRecordsPerBlock;
        }

        if (actualBlocks < 1) {
            actualBlocks = 1;
        }

        recordsPerBlock = (records.size() / actualBlocks) + 1;

        int curRecords = 0;
        int nextRecords = recordsPerBlock;
        int totalSize = records.size();
        String info = "";
        for (int i=0; i<maxBlocks && curRecords < totalSize; i++) {
            if (nextRecords > totalSize) {
                nextRecords = totalSize;
            }

            if (curRecords == nextRecords) {
                break;
            }

            //info += "cur:next("+curRecords+":"+nextRecords+")";
            List<ConsumerRecord<K,V>> tmp = records.subList(curRecords, nextRecords);
            emittedMsgs.add(tmp);
            nextCommitOffset = tmp.get(tmp.size() - 1).offset();
            curRecords = nextRecords;
            nextRecords += recordsPerBlock;
        }

        LOG.info("inputRecords:"+records.size()+"; blocks:"+emittedMsgs.size()+info);
    }

    public List<List<ConsumerRecord<K,V>>> getEmittedMsgs() {
        return emittedMsgs;
    }

    public long getFetchOffset() {
        return offsetOperator.getFetchOffset();
    }

    public long getNextCommitOffset() {
        return nextCommitOffset;
    }

    public void addMsg(KafkaSpoutMessageId messageId) {
        spoutAckCollector.addMsg(messageId);
    }

    public boolean containMsg(KafkaSpoutMessageId messageId) {
        return spoutAckCollector.containMsgId(messageId);
    }

    public void ackMsg(KafkaSpoutMessageId messageId) {
        spoutAckCollector.ackMsg(messageId);
    }

    public boolean isAcked() {
        return spoutAckCollector.isAcked();
    }

    public boolean commitAndClean() {
        if (!spoutAckCollector.isAcked()) {
            this.status = BatchStatus.WaitAck;
            return false;
        }

        long offset = getNextCommitOffset();
        boolean res = offsetOperator.commitOffset(offset);
        if (!res) {
            return false;
        } else {
            nextCommitOffset = -1;
            emittedMsgs.clear();
            spoutAckCollector.ackedAndClear();
            return true;
        }
    }

    public void failMsg(KafkaSpoutMessageId messageId) {
        retryMessageList.add(messageId);
    }

    public boolean hasRetryMessages() {
        return (!retryMessageList.isEmpty());
    }

    public Map<KafkaSpoutMessageId, List<ConsumerRecord<K,V>>> getRetryMsgs() {
        Map<KafkaSpoutMessageId, List<ConsumerRecord<K,V>>> tmp = new HashMap<>();

        for (List<ConsumerRecord<K,V>> records : emittedMsgs) {
            if (records.size() <= 0) {
                continue;
            }

            long offset = records.get(records.size() - 1).offset();
            for (KafkaSpoutMessageId msg : retryMessageList) {
                if (offset == msg.offset()) {
                    tmp.put(msg, records);
                    break;
                }
            }
        }

        return tmp;
    }

    public enum BatchStatus {
        Init("initial"),
        WaitToEmit("waitToEmit"),
        WaitAck("WaitAck"),
        WaitCommit("WaitCommit");
        private String text;

        BatchStatus(String text) {
            this.text = text;
        }

        public String getText() {
            return this.text;
        }

        // Implementing a fromString method on an enum type
        private static final Map<String, BatchStatus> stringToEnum = new HashMap<String, BatchStatus>();
        static {
            // Initialize map from constant name to enum constant
            for(BatchStatus blah : values()) {
                stringToEnum.put(blah.toString(), blah);
            }
        }

        public static BatchStatus fromString(String symbol) {
            return stringToEnum.get(symbol);
        }

        @Override
        public String toString() {
            return text;
        }
    }
}

