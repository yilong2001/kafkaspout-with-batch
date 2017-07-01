package com.storm.kafka.consumer.batched;

/**
 * Created by yilong on 2017/6/12.
 */
public class ZkTopicPartitionPath {
    final String EphemeralFlag = "EPH";
    final String PersistFlag = "PER";

    String groupId;
    String topic;
    String persistFlag;
    String ephemeralFlag;
    String rootPath;

    public ZkTopicPartitionPath(String rootPath, String groupId, String topic) {
        this.rootPath = rootPath;
        this.groupId = groupId;
        this.topic = topic;

        this.persistFlag = PersistFlag;
        this.ephemeralFlag = EphemeralFlag;
    }

    public String getPersistPath(int partition) {
        //return "/"+rootPath+"/"+groupId+"_"+topic+"_"+partition+"_"+persistFlag;
        return topic+"_"+partition+"_"+persistFlag;
    }

    public String getEphemeralPath(int partition) {
        //return "/"+rootPath+"/"+groupId+"_"+topic+"_"+partition+"_"+ephemeralFlag;
        return topic+"_"+partition+"_"+ephemeralFlag;
    }

    public String getTopic() {
        return topic;
    }
}
