package com.hw.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class Config {

    @Value(value = "${spring.kafka.streams.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.streams.application-id}")
    private String applicationId;

    @Value(value = "${spring.kafka.streams.state-dir}")
    private String stateStoreLocation;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, applicationId);
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(STATE_DIR_CONFIG, stateStoreLocation);

        return new KafkaStreamsConfiguration(props);
    }



//    @Value("${task2.topic.name}")
//    private String inputTopic;
//
//    @Bean
//    public Map<String, KStream<Integer, String>> splittingInputMessages(StreamsBuilder streamsBuilder) {
//        KStream<String, String> inputStream = streamsBuilder
//                .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
//
//        KStream<Integer, String> oneWordMessageStream = inputStream
//                .peek((key, value) -> System.out.println("Input message: " + value))
//                .filter((key, value) -> value != null && !value.trim().equals(""))
//                .flatMapValues(sentence -> Arrays.asList(sentence.split(" ")))
//                .peek((key, value) -> System.out.println("One word split message: " + value))
//                .map((key, value) -> new KeyValue<>(value.length(), value))
//                .peek((key, value) -> System.out.println("Key-value pair: key: " + key + ", value: " + value));
//
//        KStream<Integer, String> shortWordsStream = oneWordMessageStream
//                .filter((key, value) -> key < 10)
//                .peek((key, value) -> System.out.println("KStream words-short: key: " + key + ", value: " + value));
//        KStream<Integer, String> longWordsStream = oneWordMessageStream
//                .filter((key, value) -> key >= 10)
//                .peek((key, value) -> System.out.println("KStream words-long: key: " + key + ", value: " + value));
//
//        Map<String, KStream<Integer, String>> result = new HashMap<>();
//        result.put("words-short", shortWordsStream);
//        result.put("words-long", longWordsStream);
//
//        return result;
//    }
//
//    @Bean
//    public KStream<Integer, String> filterAMessageCharacter(Map<String, KStream<Integer, String>> splitMessages) {
//        KStream<Integer, String> shortWordsStream = splitMessages.get("words-short")
//                .filter((key, value) -> value.contains("a"))
//                .peek((key, value) -> System.out.println("KStream words-short after filtering 'a' character: key: " + key + ", value: " + value));
//
//        KStream<Integer, String> longWordsStream = splitMessages.get("words-long")
//                .filter((key, value) -> value.contains("a"))
//                .peek((key, value) -> System.out.println("KStream words-short after filtering 'a' character: key: " + key + ", value: " + value));
//
//        return shortWordsStream.merge(longWordsStream)
//                .peek((key, value) -> System.out.println("Result KStream with words that contains 'a' character: key: " + key + ", value: " + value));
//    }

}
