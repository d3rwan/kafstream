package com.github.d3rwan.streaming;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class SimpleAnalyzer {

    Properties props;

    public SimpleAnalyzer(String hostKafka, String hostZookeeper) {
        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-analyzer");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, hostKafka);
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, hostZookeeper);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    }

    public void run(String topic) {
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> source = builder.stream(topic);
        KTable<String, Long> wordCounts = source
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .map((key, value) -> new KeyValue<>(value, value))
                .countByKey("counts");
        wordCounts.to(Serdes.String(), Serdes.Long(), topic + "-wordcount");
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        System.out.println(" --> KStream start <-- ");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Run: SimpleAnalyzer");
        String hostKafka = args[0];
        String hostZookeeper = args[1];
        String topic = args[2];
        SimpleAnalyzer sa = new SimpleAnalyzer(hostKafka, hostZookeeper);
        sa.run(topic);
    }
}
