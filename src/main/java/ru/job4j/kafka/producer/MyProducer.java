package ru.job4j.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer {

    public static void main(String[] args) throws InterruptedException {
        KafkaProducer<String, String> producer = configureKafkaProducer();

        for (int i = 0; i < 500; i++) {
            String message = "task_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>("tasks", message);
            producer.send(record);
            Thread.sleep(100);
        }

        producer.close();
    }

    private static KafkaProducer<String, String> configureKafkaProducer() {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "localhost:9092");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer(producerProperties);
    }
}
