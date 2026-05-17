package com.kafkaexample.ProductMicroserviceForKafka.controller;

import com.kafkaexample.ProductMicroserviceForKafka.service.ProductService;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import static org.mockito.Mockito.mock;

@TestConfiguration
public class TestConfig {

    @Bean
    @Primary
    public ProductService productService() {
        return mock(ProductService.class);
    }
}

