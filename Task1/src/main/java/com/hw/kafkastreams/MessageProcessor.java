package com.hw.kafkastreams;

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

    @Value("${task1.input.topic.name}")
    private String inputTopic;

    @Value("${task1.output.topic.name}")
    private String outputTopic;

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder
                .stream(inputTopic, Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, String> processedMessages = messageStream
                .peek((k, v) -> System.out.println("Processing message: " + v));

        processedMessages.to(outputTopic);
    }

    public void setInputTopic(String inputTopic) {
        this.inputTopic = inputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

}
