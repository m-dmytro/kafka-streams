package com.hw.kafkastreams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Component
public class MessageProcessor {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Value("${task2.topic.name}")
    private String inputTopic;

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> inputStream = streamsBuilder
                .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<Integer, String> oneWordMessageStream = inputStream
                .peek((key, value) -> System.out.println("Input message: " + value))
                .filter((key, value) -> value != null && !value.trim().equals(""))
                .flatMapValues(sentence -> Arrays.asList(sentence.split(" ")))
                .peek((key, value) -> System.out.println("One word split message: " + value))
                .map((key, value) -> new KeyValue<>(value.length(), value))
                .peek((key, value) -> System.out.println("Key-value pair: key: " + key + ", value: " + value));

        KStream<Integer, String> shortWordsStream = oneWordMessageStream
                .filter((key, value) -> key < 10)
                .peek((key, value) -> System.out.println("KStream words-short: key: " + key + ", value: " + value));
        KStream<Integer, String> longWordsStream = oneWordMessageStream
                .filter((key, value) -> key >= 10)
                .peek((key, value) -> System.out.println("KStream words-long: key: " + key + ", value: " + value));

        Map<String, KStream<Integer, String>> result = new HashMap<>();
        result.put("words-short", shortWordsStream);
        result.put("words-long", longWordsStream);

        filterAMessageCharacter(result);
    }

    public KStream<Integer, String> filterAMessageCharacter(Map<String, KStream<Integer, String>> splitMessages) {
        KStream<Integer, String> shortWordsStream = splitMessages.get("words-short")
                .filter((key, value) -> value.contains("a"))
                .peek((key, value) -> System.out.println("KStream words-short after filtering 'a' character: key: " + key + ", value: " + value));

        KStream<Integer, String> longWordsStream = splitMessages.get("words-long")
                .filter((key, value) -> value.contains("a"))
                .peek((key, value) -> System.out.println("KStream words-short after filtering 'a' character: key: " + key + ", value: " + value));

        return shortWordsStream.merge(longWordsStream)
                .peek((key, value) -> System.out.println("Result KStream with words that contains 'a' character: key: " + key + ", value: " + value));
    }

}
