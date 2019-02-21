package com.ddabadi.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {


    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(ConsumerDemoGroups.class);

        String groupId = "my-fifth-app";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // earliest / latest /none

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

//        consumer.subscribe(Arrays.asList("First_topik","next_topik"));
//        consumer.subscribe(Collections.singleton("first_topic"));

        consumer.subscribe(Arrays.asList("first_topic"));

        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : consumerRecords){
                log.info("key " + record.key() + " value " + record.value());
                log.info("partition " + record.partition() + " offset " + record.offset());
            }
        }

    }
}
