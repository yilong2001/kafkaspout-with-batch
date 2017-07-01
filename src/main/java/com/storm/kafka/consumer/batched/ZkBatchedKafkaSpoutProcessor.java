package com.storm.kafka.consumer.batched;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpoutMessageId;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * Created by yilong on 2017/6/14.
 */
public class ZkBatchedKafkaSpoutProcessor<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(ZkBatchedKafkaSpoutProcessor.class);
    protected SpoutOutputCollector collector;
    private transient KafkaConsumer<K, V> kafkaConsumer;
    private KafkaSpoutStreams kafkaSpoutStreams;
    private List<ZkTopicPartitionAssigner> assigners;
    private ZooKeeper zooKeeper;

    private BatchedTuplesBuilderRepo tuplesBuilderRepo;
    private ZkBatchedKafkaSpoutConfig spoutConfig;

    Map<TopicPartition, TopicPartitionDataRepo> topicPartitionRepoMap;
    private CuratorFramework curatorFramework;


    public ZkBatchedKafkaSpoutProcessor(SpoutOutputCollector collector,
                                        KafkaSpoutStreams kafkaSpoutStream,
                                        BatchedTuplesBuilderRepo tuplesBuilderRepo,
                                        ZkBatchedKafkaSpoutConfig<K, V> spoutConfig,
                                        List<ZkTopicPartitionAssigner> assigners,
                                        CuratorFramework curatorFramework) {
        this.collector = collector;
        this.kafkaSpoutStreams = kafkaSpoutStream;
        this.tuplesBuilderRepo = tuplesBuilderRepo;
        this.spoutConfig = spoutConfig;

        this.assigners = assigners;
        this.curatorFramework = curatorFramework;
        this.topicPartitionRepoMap = new HashMap<>();
    }

    public void setKafkaConsumer(KafkaConsumer<K, V> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public void doPartitionsAssign() {
        LOG.info("****** "+Thread.currentThread().getName()+": doPartitionsAssign ");

        List<TopicPartition> tps = new ArrayList<>();
        boolean isNewPartition = false;

        for (ZkTopicPartitionAssigner assigner : assigners) {
            boolean hasAssigned = false;
            for (Map.Entry<TopicPartition, TopicPartitionDataRepo> entry : topicPartitionRepoMap.entrySet()) {
                if (entry.getKey().topic().equals(assigner.getTopic())) {
                    hasAssigned = true;
                    tps.add(entry.getKey());
                    break;
                }
            }

            if (hasAssigned) {
                continue;
            }

            int partition = assigner.assignPartiton();
            if (partition >= 0) {
                TopicPartitionDataRepo repo = new TopicPartitionDataRepo<K,V>(new ZkOffsetCommitter(curatorFramework,
                        assigner.getZkTopicPartitionPath(),
                        partition,
                        spoutConfig.getZookeeperACLs()));

                long offset = repo.getFetchOffset();

                LOG.info("****** "+Thread.currentThread().getName()+": assinged partition : "+assigner.getTopic()+":"+partition+":"+offset);
                if (offset < 0) {
                    throw new RuntimeException("assignedPartition,but offset<0");
                    //assigner.cleanPartition();
                } else {
                    isNewPartition = true;
                    TopicPartition tp = new TopicPartition(assigner.getTopic(), partition);
                    topicPartitionRepoMap.put(tp, repo);
                    tps.add(tp);
                }
            }
        }

        if (isNewPartition) {
            kafkaConsumer.assign(tps);
            for (TopicPartition tp : tps) {
                if (!topicPartitionRepoMap.containsKey(tp)) {
                    throw new RuntimeException("partiton assigned, but topicpartitionrepo no data!");
                }

                TopicPartitionDataRepo tpp = topicPartitionRepoMap.get(tp);
                long off = tpp.getFetchOffset();
                LOG.info("****** "+Thread.currentThread().getName()+": doFirstSeek "+ tp.toString() + " : " + off);
                kafkaConsumer.seek(tp, off);
            }
        }
    }

    public void doCommit() {
        for (Map.Entry<TopicPartition, TopicPartitionDataRepo> entry : topicPartitionRepoMap.entrySet()) {
            if (entry.getValue().getStatus() != TopicPartitionDataRepo.BatchStatus.WaitCommit) {
                LOG.info("****** : cannotCommit:" + entry.getKey().partition() + ":" + entry.getKey().partition() + ":" + entry.getValue().getStatus().toString());
                continue;
            }

            LOG.info("****** " + Thread.currentThread().getName() + ":doCommit:");

            if (!entry.getValue().commitAndClean()) {
                LOG.error("****** " + Thread.currentThread().getName() + ": cannot perform zookeeper commit : " + entry.getKey().topic() + ":" + entry.getKey().partition());
            } else {
                entry.getValue().setStatus(TopicPartitionDataRepo.BatchStatus.Init);
            }
        }
    }

    public void doPoll() {
        for (Map.Entry<TopicPartition, TopicPartitionDataRepo> entry : topicPartitionRepoMap.entrySet()) {
            if (entry.getValue().getStatus() != TopicPartitionDataRepo.BatchStatus.Init) {
                LOG.info("******: cannotPoll:"+entry.getKey().partition()+":"+entry.getKey().partition()+":"+entry.getValue().getStatus().toString());
                continue;
            }

            int curRecords = 0;
            long curTS = System.currentTimeMillis();

            kafkaConsumer.assign(Arrays.asList(entry.getKey()));
            //long fetchOffset = entry.getValue().getFetchOffset();
            //kafkaConsumer.seek(entry.getKey(), fetchOffset);

            List<ConsumerRecord<K, V>> partitionRecords = new ArrayList<>();

            while (curRecords < spoutConfig.getMaxRecordsPerBatch() &&
                    (curTS + spoutConfig.getMaxPollTimeoutMsPerBatch()) > System.currentTimeMillis()) {
                ConsumerRecords<K, V> records = kafkaConsumer.poll(spoutConfig.getPollDurationMsPerOnce());
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<K, V>> rds = records.records(partition);
                    curRecords += rds.size();
                    for (ConsumerRecord<K, V> rd : rds) {
                        partitionRecords.add(rd);
                    }
                }
            }

            int size = partitionRecords.size();
            if (size > 0) {
                long offset = partitionRecords.get(size-1).offset();
                LOG.info("****** " + "partition: " + entry.getKey().partition() + "; latestOffset:" + offset);
            }

            LOG.info("****** "+Thread.currentThread().getName()+": pollCount: "+curRecords);

            if (curRecords > 0) {
                entry.getValue().addRecords(partitionRecords);
                entry.getValue().setStatus(TopicPartitionDataRepo.BatchStatus.WaitToEmit);
            }
        }
    }

    //how to keep sequenced Record(with key) be to sequencing ???
    public void doEmit() {
        for (Map.Entry<TopicPartition, TopicPartitionDataRepo> entry : topicPartitionRepoMap.entrySet()) {
            if (entry.getValue().getStatus() != TopicPartitionDataRepo.BatchStatus.WaitToEmit &&
                    entry.getValue().getStatus() != TopicPartitionDataRepo.BatchStatus.WaitAck) {
                LOG.info("******: cannotEmit:"+entry.getKey().partition()+":"+entry.getKey().partition()+":"+entry.getValue().getStatus().toString());
                continue;
            }

            int emitCount = 0;
            if (entry.getValue().getStatus() == TopicPartitionDataRepo.BatchStatus.WaitToEmit) {
                List<List<ConsumerRecord<K, V>>> ttmp = entry.getValue().getEmittedMsgs();
                LOG.info("emittedMsgs : "+ttmp.size());
                for (List<ConsumerRecord<K, V>> records : ttmp) {
                    final long offset = (records.size()>0)?records.get(records.size() - 1).offset():0;
                    final KafkaSpoutMessageId msgId = new KafkaSpoutMessageId(entry.getKey(), offset);

                    final List<Object> tuple = spoutConfig.getTuplesBuilderRepo()
                            .getTupleBuilder(entry.getKey().topic())
                            .buildBatchedTuple(records);
                    kafkaSpoutStreams.emit(collector, tuple, msgId);

                    entry.getValue().addMsg(msgId);
                    emitCount += records.size();
                }

                entry.getValue().setStatus(TopicPartitionDataRepo.BatchStatus.WaitAck);
            } else {
                //wait ack
                if (entry.getValue().hasRetryMessages()) {
                    Map<KafkaSpoutMessageId, List<ConsumerRecord<K, V>>> ttmp = entry.getValue().getRetryMsgs();
                    for(Map.Entry<KafkaSpoutMessageId, List<ConsumerRecord<K, V>>> tmp : ttmp.entrySet()) {

                        final List<Object> tuple = spoutConfig.getTuplesBuilderRepo()
                                .getTupleBuilder(entry.getKey().topic())
                                .buildBatchedTuple(tmp.getValue());
                        kafkaSpoutStreams.emit(collector, tuple, tmp.getKey());

                        emitCount += tmp.getValue().size();
                    }
                }
            }

            LOG.info("****** "+Thread.currentThread().getName()+": doEmit:emitCount : "+emitCount);
        }
    }

    public void onAck(Object messageId) {
        boolean isInQueueMsg = false;
        for (Map.Entry<TopicPartition, TopicPartitionDataRepo> entry : topicPartitionRepoMap.entrySet()) {
            if (entry.getValue().getStatus() != TopicPartitionDataRepo.BatchStatus.WaitAck) {
                LOG.info("******: cannotAck:"+entry.getKey().partition()+":"+entry.getKey().partition()+":"+entry.getValue().getStatus().toString());
                continue;
            }

            if (entry.getValue().containMsg((KafkaSpoutMessageId)messageId)) {
                isInQueueMsg = true;
                entry.getValue().ackMsg((KafkaSpoutMessageId)messageId);
            }

            if (isInQueueMsg && entry.getValue().isAcked()) {
                entry.getValue().setStatus(TopicPartitionDataRepo.BatchStatus.WaitCommit);
            }

            if (isInQueueMsg) {
                break;
            }
        }

        if (!isInQueueMsg) {
            LOG.error("****** "+Thread.currentThread().getName()+":messageId: not found onAck : " + messageId.toString());
        }
    }

    public void onFail(Object messageId) {
        boolean isInQueueMsg = false;
        final KafkaSpoutMessageId msgId = (KafkaSpoutMessageId) messageId;

        for (Map.Entry<TopicPartition, TopicPartitionDataRepo> entry : topicPartitionRepoMap.entrySet()) {
            if (entry.getValue().getStatus() != TopicPartitionDataRepo.BatchStatus.WaitAck) {
                continue;
            }

            if (entry.getValue().containMsg((KafkaSpoutMessageId)messageId)) {
                isInQueueMsg = true;
                entry.getValue().failMsg(msgId);
            }
        }

        if (!isInQueueMsg) {
            LOG.error("****** "+Thread.currentThread().getName()+": messageId not found onFailed : " + messageId.toString());
        }
    }

    public void addTpoicPartition(TopicPartition tp, ZkOffsetCommitter offsetOperator) {
        TopicPartitionDataRepo repo = new TopicPartitionDataRepo(offsetOperator);
        topicPartitionRepoMap.put(tp, repo);
    }

    public void cleanAll() {

    }

    public String toString() {
        String out = "";
        for (Map.Entry<TopicPartition, TopicPartitionDataRepo> entry : topicPartitionRepoMap.entrySet()) {
            out += entry.getKey().topic()+":"+entry.getKey().partition();
            out += ":"+entry.getValue().getFetchOffset();
        }

        return out;
    }

}
