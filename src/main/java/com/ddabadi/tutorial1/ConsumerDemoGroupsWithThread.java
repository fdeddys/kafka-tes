package com.ddabadi.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoGroupsWithThread {


    public static void main(String[] args) {


//        consumer.subscribe(Arrays.asList("First_topik","next_topik"));
//        consumer.subscribe(Collections.singleton("first_topic"));
//        KafkaConsumer<String, String> consumer =  new KafkaConsumer<String, String>(properties);

        new ConsumerDemoGroupsWithThread().run();
    }

    private ConsumerDemoGroupsWithThread() {
    }

    private void run(){
        Logger log = LoggerFactory.getLogger(ConsumerDemoGroupsWithThread.class);
        String groupId = "my-sixth-app";
        CountDownLatch latch = new CountDownLatch(1);

        log.info("creating consumer thread");
        Runnable myConsumerRunnable = new ConsummerRunnable (groupId, latch);

        Thread mythread = new Thread(myConsumerRunnable);
        mythread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            log.info("caught shutdown");
            ((ConsummerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
                log.error("app in interup");
            } finally {
                log.info("app is closing");
            }
            log.info("application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.error("app in interup");
        } finally {
            log.info("app is closing");
        }

    }

    public class ConsummerRunnable implements Runnable{

        private CountDownLatch latch;
        KafkaConsumer<String, String> consumer;
        Logger log = LoggerFactory.getLogger(ConsummerRunnable.class);

        public ConsummerRunnable(String groupId, CountDownLatch latch) {
            this.latch = latch;

            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            // earliest / latest /none
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList("first_topic"));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        log.info("key " + record.key() + " value " + record.value());
                        log.info("partition " + record.partition() + " offset " + record.offset());
                    }
                }
            } catch (WakeupException e){
                log.info("receive shutdown signal");
            }  finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown(){
            log.info("processing shuting down");
            consumer.wakeup();
        }
    }

}
