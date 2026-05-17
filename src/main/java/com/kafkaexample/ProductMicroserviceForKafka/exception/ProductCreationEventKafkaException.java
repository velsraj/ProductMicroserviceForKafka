package com.kafkaexample.ProductMicroserviceForKafka.exception;

public class ProductCreationEventKafkaException extends RuntimeException {

    public ProductCreationEventKafkaException(String message) {
        super(message);
    }
}
