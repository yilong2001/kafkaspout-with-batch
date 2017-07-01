# kafkaspout-with-batch
a batched kafkaspout with self-managed offsets in zookeeper 

## There are some issues with org.apache.storm.kafka.spout.KafkaSpout.
- 1. When the topology is running in multi workers in different supervisors, it is very often to trigger kafkaspout rebalance. And so the streaming is not stable. And it will cause massive retransmission of lost packets.
- 2. When maxUncommittedOffsets is less than 200000 (for limited flow), sometimes there is deadlock. The phenomenon is the heartbeat between spout and kafka can not be performed.
- 3. When the data is from storm to hbase,  batch is used to improve writing productivity. So using batch from spout to bolt is better for special scene.

## a new kafkaspout with self-managed offsets in zookeeper
- 1. self-managed offsets in zookeeper to avoid frequent rebalance.
- 2. batched records from spout to bolt.
- 3. it is more suitable for data store such as hbase.

## 新的 kafka spout 实例
- 1. 使用 zookeeper 管理 offset
- 2. spout 批量处理从 kafka 读取的记录
- 3. 批量数据处理，在类似于写入hbase的场景，更加适用
- 4. 解决 kafkaspout 经常发生的 rebalance 问题

## 联系 yilong2001@126.com
