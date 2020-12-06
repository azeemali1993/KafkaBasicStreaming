package org.azali.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class producerDemo {
    public static void main(String[] args) {
        System.out.println("Hellow World!!");

        String BootstrapServer = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create producer data:
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic", "hello World");

        //sendin data

        producer.send(record);

        producer.flush();
        producer.close();


    }
}
