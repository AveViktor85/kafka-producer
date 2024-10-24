package org.example;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        producerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
//        producerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for (Integer i = 0; i < 25000; i++) {
            producer.send(new ProducerRecord<>("my-topic", i + " " + RandomStringUtils.randomAlphanumeric(10, 20)));
            System.out.println(i);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
        producer.close();
        System.out.println( "Hello World!" );
    }
}
