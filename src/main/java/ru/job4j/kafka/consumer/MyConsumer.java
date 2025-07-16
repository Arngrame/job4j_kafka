package ru.job4j.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {

    public static void main(String[] args) {
        KafkaConsumer kafkaConsumer = configureKafkaConsumer();
        kafkaConsumer.subscribe(Arrays.asList("tasks"));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Message has received: offset = %d, partition = %d, key = %s, value = %s%n",
                        record.offset(), record.partition(), record.key(), record.value());
            }
        }
    }

    private static KafkaConsumer configureKafkaConsumer() {
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", "localhost:9092");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("group.id", "my-consumer-group");
        consumerProperties.put("auto.offset.reset", "earliest");

        return new KafkaConsumer(consumerProperties);
    }

}
