package com.hw.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageProcessorTest {

    private MessageProcessor messageProcessor;

    @BeforeEach
    void setUp() {
        messageProcessor = new MessageProcessor();
    }

    @Test
    void givenInputMessages_whenProcessed_thenWordCountIsProduced() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        messageProcessor.setInputTopic("input-topic");
        messageProcessor.setOutputTopic("output-topic");
        messageProcessor.buildPipeline(streamsBuilder);
        Topology topology = streamsBuilder.build();

        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {
            TestInputTopic<String, String> inputTopic = topologyTestDriver
                    .createInputTopic("input-topic", Serdes.String().serializer(), Serdes.String().serializer());

            TestOutputTopic<String, String> outputTopic = topologyTestDriver
                    .createOutputTopic("output-topic", Serdes.String().deserializer(), Serdes.String().deserializer());

            inputTopic.pipeInput("key", "hello world");
            inputTopic.pipeInput("key2", "hello");

            assertThat(outputTopic.readKeyValuesToList())
                    .containsExactly(
                            KeyValue.pair("key", "hello world"),
                            KeyValue.pair("key2", "hello")
                    );
        }
    }

}
