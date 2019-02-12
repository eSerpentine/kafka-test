package com.eserpentine.kafka.partitioners;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        System.out.println("Partitions count: " + partitionInfos.size());
        int size = partitionInfos.size();

        if (keyBytes == null || !(key instanceof Long)) {
            throw new InvalidRecordException("We expect all messages" +
                    "to have customer name as key");
        }

        Long keyLong = (Long) key;

        int nextPartition = new Random().nextInt(size);

        System.out.println("Next partition is nextPartition: " + nextPartition);

        return nextPartition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
