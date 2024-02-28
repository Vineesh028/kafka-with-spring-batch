package com.batch.sample.model;

import java.io.IOException;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.stereotype.Component;

import com.batch.sample.config.ObjectMapperFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.NoArgsConstructor;

@Component
@NoArgsConstructor
public class UserDeserializer implements Deserializer<User> {

	ObjectMapper objectMapper = ObjectMapperFactory.getObjectMapperInstance();

    @Override
    public User deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            } else {
                return objectMapper.readValue(data, User.class);
            }
        } catch (SerializationException | IOException e) {
            throw new SerializationException("Error serializing user", e);
        }
    }
}