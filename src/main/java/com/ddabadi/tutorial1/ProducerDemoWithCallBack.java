package com.ddabadi.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    public static void main(String[] args) {

        final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=1; i<=10; i++) {
            // producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic","hello world " + Integer.toString(i));
            // send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        log.info("meta data \n " +
                                "topic = " + metadata.topic() + " \n" +
                                "partition = " + metadata.partition() + " \n" +
                                "offset = " + metadata.offset() + " \n" +
                                "timestamp = " + metadata.timestamp() + " \n");
                    } else {
                        log.error(exception.getMessage());
                    }
                }
            });
        }

        producer.flush();
        producer.close();
    }

}
