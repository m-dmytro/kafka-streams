package com.hw.kafkastreams.service;

import com.hw.kafkastreams.model.Person;
import com.hw.kafkastreams.serdes.CustomSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MessageProcessor {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Value("${task4.input.topic.name}")
    private String inputTopic;

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, Person> inputStream = streamsBuilder
                .stream(inputTopic, Consumed.with(STRING_SERDE, CustomSerdes.Person()));

        inputStream
                .peek((key, value) -> System.out.println("Input message: key: '" + key + "'; value: '" + value + "'"))
                .filter((key, value) -> value != null && !value.isEmptyObj())
                .peek((key, value) -> System.out.println("Filtered message: key: '" + key + "'; value: '" + value + "'"));
//              .to("output-topic", Produced.with(Serdes.String(), CustomSerdes.Person()));
    }

}
