package com.hw.kafkastreams.serdes;

import com.hw.kafkastreams.model.Person;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class CustomSerdes {

    private CustomSerdes() {}

    public static Serde<Person> Person() {
        JsonSerializer<Person> serializer = new JsonSerializer<>();
        JsonDeserializer<Person> deserializer = new JsonDeserializer<>(Person.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

}
