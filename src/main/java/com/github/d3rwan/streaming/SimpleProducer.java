package com.github.d3rwan.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleProducer {

    private Properties props;

    public SimpleProducer(String host) {
        props = new Properties();
        props.put("metadata.broker.list", host);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
    }

    public void run(String topic, String message) {
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        ProducerRecord<Integer, String> data = new ProducerRecord<>(topic, message);
        System.out.println("Send>>> " + data);
        producer.send(data);
        producer.close();
    }

    public static void main(String[] args) {
        String host = args[0];
        String topic = args[1];
        String message = args[2];
        SimpleProducer sp = new SimpleProducer(host);
        sp.run(topic, message);
    }
}
