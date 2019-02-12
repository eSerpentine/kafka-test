package com.eserpentine.kafka;


import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.*;

import static com.eserpentine.kafka.utils.CommonUtils.*;
import static com.eserpentine.kafka.utils.CommonUtils.waitBeforeSend;

public class KafkaTest {

    public static final String TOPIC = "my-topic15";
    public static final String KAFKA_SERVER = "localhost:9092";

    private static void correctSetup() {
        Properties topicConfig = new Properties();
        topicConfig.put("bootstrap.servers", KAFKA_SERVER);

        Map<String, String> config = new HashMap<>();
        config.put("num.partitions", "3");

        short partitions = 3;
        short replication = 2;

        AdminClient client = AdminClient.create(topicConfig);
        client.createTopics(Collections.singletonList(new NewTopic(TOPIC, partitions, replication)));
        client.createPartitions(new HashMap<String, NewPartitions>(){{put(TOPIC,NewPartitions.increaseTo(3));}});
    }

    public static void main(String[] args) {
        correctSetup();
        producer("producer1");
        consumer("consumer1", "group1");
        consumer("consumer2", "group1");
    }

    private static void producer(String producerName) {
        threadStart(() -> {

            Properties properties = new Properties();
            properties.put("bootstrap.servers", KAFKA_SERVER);
            properties.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("partitioner.class", "com.eserpentine.kafka.partitioners.CustomPartitioner");
            properties.put("acks", "0");

            KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);

            int i = 0;
            while (true) {
                waitBeforeSend(2);
                ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, Long.valueOf(System.currentTimeMillis()),  String.valueOf(++i));
                producer.send(record);
                System.out.println("Producer: " + producerName + " send message");
            }
        });
    }

    private static void consumer(String name, String consumerGroup) {
        threadStart(() -> {

            Properties properties = new Properties();
            properties.put("bootstrap.servers", KAFKA_SERVER);
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("group.id", consumerGroup);

            KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                ConsumerRecords<Long, String> polls = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<Long, String> poll : polls) {
                    System.out.println("Consumer: " + name + ": " + poll.key());
                    System.out.println("Consumer: " + name + ": " + poll.value());
                }
            }
        });
    }
}
