package com.eserpentine.kafka;


import com.eserpentine.kafka.utils.CommonUtils;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.eserpentine.kafka.utils.CommonUtils.*;
import static com.eserpentine.kafka.utils.CommonUtils.waitBeforeSend;

public class KafkaTest {

    public static final String MY_TOPIC_1 = "my-topic3";
    public static final String KAFKA_SERVER = "localhost:9092";
    public static final String ZOOKEEPER_CONNECT = "localhost:2181";

    private static void setup() {
        String zookeeperConnect = ZOOKEEPER_CONNECT;
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;

        String topic = MY_TOPIC_1;
        int partitions = 2;
        int replication = 2;
        Properties topicConfig = new Properties(); // add per-topic configurations settings here

        ZkClient zkClient = new ZkClient(
                zookeeperConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);

        boolean isSecureKafkaCluster = false;

        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
        AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig, RackAwareMode.Enforced$.MODULE$);
        zkClient.close();
    }

    public static void main(String[] args) {
        setup();
        producer("producer1");
        consumer("consumer1");
        consumer("consumer2");
    }

    private static void producer(String producerName) {
        threadStart(() -> {

            Properties properties = new Properties();
            properties.put("bootstrap.servers", KAFKA_SERVER);
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("acks", "0");

            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

            int i = 0;
            while (true) {
                waitBeforeSend(2);
                ProducerRecord<String, String> record = new ProducerRecord<>(MY_TOPIC_1, null,  String.valueOf(++i));
                producer.send(record);
                System.out.println("Producer: " + producerName + " send message");
            }
        });
    }

    private static void consumer(String name) {
        threadStart(() -> {

            Properties properties = new Properties();
            properties.put("bootstrap.servers", KAFKA_SERVER);
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("group.id", "my-topic");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singletonList(MY_TOPIC_1));

            while (true) {
                ConsumerRecords<String, String> polls = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> poll : polls) {
                    System.out.println("Consumer: " + name + ": " + poll.key());
                    System.out.println("Consumer: " + name + ": " + poll.value());
                }
            }
        });
    }
}
