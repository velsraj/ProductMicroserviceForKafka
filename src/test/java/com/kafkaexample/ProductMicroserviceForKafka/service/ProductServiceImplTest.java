package com.kafkaexample.ProductMicroserviceForKafka.service;

import com.kafkaexample.ProductMicroserviceForKafka.exception.ProductCreationEventKafkaException;
import com.kafkaexample.ProductMicroserviceForKafka.model.request.ProductCreatedEvent;
import com.kafkaexample.ProductMicroserviceForKafka.model.request.ProductCreationRequest;
import com.kafkaexample.ProductMicroserviceForKafka.model.response.ProductCreationResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProductServiceImplTest {

    @Mock
    private KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    @InjectMocks
    private ProductServiceImpl productService;

    private ProductCreationRequest productCreationRequest;

    @BeforeEach
    void setUp() {
        productCreationRequest = ProductCreationRequest.builder()
                .title("Test Product")
                .price(new BigDecimal("99.99"))
                .quantity(10)
                .build();
    }

    @Test
    void testCreateProduct_Success() {
        // Arrange
        @SuppressWarnings("unchecked")
        SendResult<String, ProductCreatedEvent> sendResult = mock(SendResult.class);
        org.apache.kafka.clients.producer.RecordMetadata recordMetadata = mock(
                org.apache.kafka.clients.producer.RecordMetadata.class
        );

        when(recordMetadata.partition()).thenReturn(0);
        when(recordMetadata.topic()).thenReturn("product-created-events-topic");
        when(recordMetadata.offset()).thenReturn(100L);
        when(sendResult.getRecordMetadata()).thenReturn(recordMetadata);

        CompletableFuture<SendResult<String, ProductCreatedEvent>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(eq("product-created-events-topic"), anyString(), any(ProductCreatedEvent.class)))
                .thenReturn(future);

        // Act
        ProductCreationResponse response = productService.createProduct(productCreationRequest);

        // Assert
        assertNotNull(response);
        assertNotNull(response.getProductId());
        verify(kafkaTemplate, times(1)).send(
                eq("product-created-events-topic"),
                anyString(),
                any(ProductCreatedEvent.class)
        );
    }

    @Test
    void testCreateProduct_WithCorrectEventData() {
        // Arrange
        @SuppressWarnings("unchecked")
        SendResult<String, ProductCreatedEvent> sendResult = mock(SendResult.class);
        org.apache.kafka.clients.producer.RecordMetadata recordMetadata = mock(
                org.apache.kafka.clients.producer.RecordMetadata.class
        );

        when(recordMetadata.partition()).thenReturn(0);
        when(recordMetadata.topic()).thenReturn("product-created-events-topic");
        when(recordMetadata.offset()).thenReturn(100L);
        when(sendResult.getRecordMetadata()).thenReturn(recordMetadata);

        CompletableFuture<SendResult<String, ProductCreatedEvent>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(eq("product-created-events-topic"), anyString(), any(ProductCreatedEvent.class)))
                .thenReturn(future);

        // Act
        ProductCreationResponse response = productService.createProduct(productCreationRequest);

        // Assert
        assertNotNull(response);
        assertNotNull(response.getProductId());

        // Verify that the event data is correctly sent
        verify(kafkaTemplate, times(1)).send(
                eq("product-created-events-topic"),
                eq(response.getProductId()),
                argThat(event ->
                    event.getTitle().equals("Test Product") &&
                    event.getPrice().equals(new BigDecimal("99.99")) &&
                    event.getQuantity().equals(10) &&
                    event.getProductId().equals(response.getProductId())
                )
        );
    }

    @Test
    void testCreateProduct_InterruptedException() {
        // Arrange
        CompletableFuture<SendResult<String, ProductCreatedEvent>> future = new CompletableFuture<>();
        future.completeExceptionally(new InterruptedException("Test interruption"));

        when(kafkaTemplate.send(eq("product-created-events-topic"), anyString(), any(ProductCreatedEvent.class)))
                .thenReturn(future);

        // Act & Assert
        assertThrows(ProductCreationEventKafkaException.class,
                () -> productService.createProduct(productCreationRequest));

        verify(kafkaTemplate, times(1)).send(
                eq("product-created-events-topic"),
                anyString(),
                any(ProductCreatedEvent.class)
        );
    }

    @Test
    void testCreateProduct_ExecutionException_KafkaException() {
        // Arrange
        CompletableFuture<SendResult<String, ProductCreatedEvent>> future = new CompletableFuture<>();
        org.apache.kafka.common.KafkaException kafkaException = new org.apache.kafka.common.KafkaException("Kafka error");
        future.completeExceptionally(new ExecutionException(kafkaException));

        when(kafkaTemplate.send(eq("product-created-events-topic"), anyString(), any(ProductCreatedEvent.class)))
                .thenReturn(future);

        // Act & Assert
        assertThrows(ProductCreationEventKafkaException.class,
                () -> productService.createProduct(productCreationRequest));

        verify(kafkaTemplate, times(1)).send(
                eq("product-created-events-topic"),
                anyString(),
                any(ProductCreatedEvent.class)
        );
    }

    @Test
    void testCreateProduct_ExecutionException_TimeoutException() {
        // Arrange
        CompletableFuture<SendResult<String, ProductCreatedEvent>> future = new CompletableFuture<>();
        org.apache.kafka.common.errors.TimeoutException timeoutException =
                new org.apache.kafka.common.errors.TimeoutException("Request timed out");
        future.completeExceptionally(new ExecutionException(timeoutException));

        when(kafkaTemplate.send(eq("product-created-events-topic"), anyString(), any(ProductCreatedEvent.class)))
                .thenReturn(future);

        // Act & Assert
        assertThrows(ProductCreationEventKafkaException.class,
                () -> productService.createProduct(productCreationRequest));

        verify(kafkaTemplate, times(1)).send(
                eq("product-created-events-topic"),
                anyString(),
                any(ProductCreatedEvent.class)
        );
    }

    @Test
    void testCreateProduct_ExecutionException_SerializationException() {
        // Arrange
        CompletableFuture<SendResult<String, ProductCreatedEvent>> future = new CompletableFuture<>();
        org.apache.kafka.common.errors.SerializationException serializationException =
                new org.apache.kafka.common.errors.SerializationException("Serialization failed");
        future.completeExceptionally(new ExecutionException(serializationException));

        when(kafkaTemplate.send(eq("product-created-events-topic"), anyString(), any(ProductCreatedEvent.class)))
                .thenReturn(future);

        // Act & Assert
        assertThrows(ProductCreationEventKafkaException.class, () -> {
            productService.createProduct(productCreationRequest);
        });

        verify(kafkaTemplate, times(1)).send(
                eq("product-created-events-topic"),
                anyString(),
                any(ProductCreatedEvent.class)
        );
    }

    @Test
    void testCreateProduct_MultipleRequests() {
        // Arrange
        SendResult<String, ProductCreatedEvent> sendResult = mock(SendResult.class);
        org.apache.kafka.clients.producer.RecordMetadata recordMetadata = mock(
                org.apache.kafka.clients.producer.RecordMetadata.class
        );

        when(recordMetadata.partition()).thenReturn(0);
        when(recordMetadata.topic()).thenReturn("product-created-events-topic");
        when(recordMetadata.offset()).thenReturn(100L);
        when(sendResult.getRecordMetadata()).thenReturn(recordMetadata);

        CompletableFuture<SendResult<String, ProductCreatedEvent>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(eq("product-created-events-topic"), anyString(), any(ProductCreatedEvent.class)))
                .thenReturn(future);

        // Act
        ProductCreationResponse response1 = productService.createProduct(productCreationRequest);
        ProductCreationResponse response2 = productService.createProduct(productCreationRequest);

        // Assert
        assertNotNull(response1);
        assertNotNull(response2);
        assertNotEquals(response1.getProductId(), response2.getProductId());
        verify(kafkaTemplate, times(2)).send(
                eq("product-created-events-topic"),
                anyString(),
                any(ProductCreatedEvent.class)
        );
    }
}






