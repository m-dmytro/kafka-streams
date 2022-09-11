package com.hw.kafkastreams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class MessageProcessor {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Value("${task3.input.topic.name.1}")
    private String inputTopic1;

    @Value("${task3.input.topic.name.2}")
    private String inputTopic2;

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> inputStream1 = streamsBuilder
                .stream(inputTopic1, Consumed.with(STRING_SERDE, STRING_SERDE))
                .peek((key, value) -> System.out.println("Input message from stream1: key: '" + key + "'; value: '" + value + "'"))
                .filter((key, value) -> value != null && !value.trim().equals("") && value.contains(":"))
                .map((key, value) -> {
                    String[] keyValuePair = value.split(":");
                    return new KeyValue<>(keyValuePair[0].trim(), keyValuePair[1].trim());
                })
                .peek((key, value) -> System.out.println("Input message from stream1: key: '" + key + "'; value: '" + value + "'"));

        KStream<String, String> inputStream2 = streamsBuilder
                .stream(inputTopic2, Consumed.with(STRING_SERDE, STRING_SERDE))
                .peek((key, value) -> System.out.println("Input message from stream2: key: '" + key + "'; value: '" + value + "'"))
                .filter((key, value) -> value != null && !value.trim().equals("") && value.contains(":"))
                .map((key, value) -> {
                    String[] keyValuePair = value.split(":");
                    return new KeyValue<>(keyValuePair[0].trim(), keyValuePair[1].trim());
                })
                .peek((key, value) -> System.out.println("Input message from stream2: key: '" + key + "'; value: '" + value + "'"));

        ValueJoiner<String, String, String> valueJoiner = (leftValue, rightValue) -> {
            return "'" + leftValue + "' '" + rightValue + "'";
        };

        inputStream1
                .join(
                        inputStream2,
                        valueJoiner,
                        JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(60), Duration.ofSeconds(30)))
                .peek((key, value) -> System.out.println("Joined messages: key: " + key + "; value: " + value));
    }

}
