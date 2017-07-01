package com.storm.kafka.consumer.conf;

import org.apache.storm.Config;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MyTopoConf {
	private static final Logger LOG = LoggerFactory.getLogger(MyTopoConf.class);

	private final String zookeeperQuorum = "localhost";
	private final String nimbusServers = "localhost";
	private final String bootstrapServers = "localhost:9092";

	private final String groupId = "consumerX013";
	
	private static MyTopoConf topoConfInstance = new MyTopoConf();
	private MyTopoConf(){}
	
	public static MyTopoConf newTopoConf(){
		return topoConfInstance;
	}

    public static Logger getLog() {
        return LOG;
    }

    public static MyTopoConf getTopoConfInstance() {
        return topoConfInstance;
    }

    public Map<String, Object> getKafkaConsumerProps() {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, bootstrapServers);
        props.put(KafkaSpoutConfig.Consumer.GROUP_ID, groupId);
        props.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        props.put("max.partition.fetch.bytes", 3*1024);
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        //props.put("heartbeat.interval.ms", "3000");

        //props.put("security.protocol", "SASL_PLAINTEXT");
        //props.put("sasl.kerberos.service.name", "kafka");

        return props;
    }
    
    public Config getTopoConf() {
    	Config config = new Config();
		List<String> zks = Arrays.asList(zookeeperQuorum.split(","));
		List<String> nimbus = Arrays.asList(nimbusServers.split(","));
		config.put(Config.STORM_ZOOKEEPER_SERVERS, zks);
		config.put(Config.NIMBUS_SEEDS, nimbus);

        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 2048);
        config.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, false);
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);

        config.setNumAckers(1);

        return config;
    }
    
    public List<String> getTopics() {
        List<String> tpoics = new ArrayList<String>();
        tpoics.add("multihbase10");
        return tpoics;
    }

    public String getBoltsStreamId() {
        return "BoltsStream-"+groupId;
    }

    public String getZkRootPath() {
	    return "mycheckpoint";
    }

    public String getZkServer() {
	    return "localhost:2181";
    }

    public List<ACL> getACLs() {
        List<ACL> acls = new ArrayList<ACL>();
        acls.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE));
        return acls;
    }

}
