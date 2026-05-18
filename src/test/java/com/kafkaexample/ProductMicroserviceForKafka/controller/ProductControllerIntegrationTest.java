package com.kafkaexample.ProductMicroserviceForKafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaexample.ProductMicroserviceForKafka.exception.ProductCreationEventKafkaException;
import com.kafkaexample.ProductMicroserviceForKafka.model.request.ProductCreationRequest;
import com.kafkaexample.ProductMicroserviceForKafka.model.response.ProductCreationResponse;
import com.kafkaexample.ProductMicroserviceForKafka.service.ProductService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Import(TestConfig.class)
class ProductControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ProductService productService;

    private ProductCreationRequest productCreationRequest;

    @BeforeEach
    void setUp() {
        productCreationRequest = ProductCreationRequest.builder()
                .title("Integration Test Product")
                .price(new BigDecimal("149.99"))
                .quantity(25)
                .build();
        reset(productService);
    }

    @Test
    void testCreateProduct_Success_Returns201() throws Exception {
        String productId = UUID.randomUUID().toString();
        ProductCreationResponse response = ProductCreationResponse.builder()
                .productId(productId)
                .build();

        when(productService.createProduct(any(ProductCreationRequest.class))).thenReturn(response);

        mockMvc.perform(post("/v1/products")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(productCreationRequest)))
                .andExpect(status().isCreated())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.productId", notNullValue()))
                .andExpect(jsonPath("$.productId", equalTo(productId)));
    }

    @Test
    void testCreateProduct_ServiceThrowsKafkaException_Returns400() throws Exception {
        doThrow(new ProductCreationEventKafkaException("Failed to send Kafka event"))
                .when(productService).createProduct(any(ProductCreationRequest.class));

        mockMvc.perform(post("/v1/products")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(productCreationRequest)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void testCreateProduct_MissingPrice_Returns400() throws Exception {
        ProductCreationRequest invalid = ProductCreationRequest.builder()
                .title("No Price Product")
                .quantity(5)
                .build();

        mockMvc.perform(post("/v1/products")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(invalid)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void testCreateProduct_InvalidContentType_Returns415() throws Exception {
        mockMvc.perform(post("/v1/products")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("not a json"))
                .andExpect(status().isUnsupportedMediaType());
    }

    @Test
    void testCreateProduct_MalformedJson_Returns400() throws Exception {
        mockMvc.perform(post("/v1/products")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{invalid json"))
                .andExpect(status().isBadRequest());
    }
}

