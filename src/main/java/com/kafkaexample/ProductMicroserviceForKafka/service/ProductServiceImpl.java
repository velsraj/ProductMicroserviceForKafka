package com.kafkaexample.ProductMicroserviceForKafka.service;

import com.kafkaexample.ProductMicroserviceForKafka.exception.ProductCreationEventKafkaException;
import com.kafkaexample.ProductMicroserviceForKafka.model.request.ProductCreationRequest;
import com.kafkaexample.ProductMicroserviceForKafka.model.request.ProductCreatedEvent;
import com.kafkaexample.ProductMicroserviceForKafka.model.response.ProductCreationResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductServiceImpl implements ProductService {
	
	private final KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

	@Override
	public ProductCreationResponse createProduct(ProductCreationRequest productRestModel) {
		String productId = UUID.randomUUID().toString();
		// TODO: Persist Product Details into database table before publishing an Event
		ProductCreatedEvent productCreatedEvent = ProductCreatedEvent.builder().productId(productId)
				.title(productRestModel.getTitle())
				.price(productRestModel.getPrice())
				.quantity(productRestModel.getQuantity())
				.build();
		try {
			SendResult<String, ProductCreatedEvent> result =
					kafkaTemplate.send("product-created-events-topic", productId, productCreatedEvent)
							.get();
			log.info("Partition : {}, Topic : {}, Offset: {}",
					result.getRecordMetadata().partition(),
					result.getRecordMetadata().topic(),
					result.getRecordMetadata().offset());
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt(); // restore interrupt status
			log.error("Thread interrupted while sending Kafka event", ie);
			throw new ProductCreationEventKafkaException("Interrupted while sending Kafka event");
		} catch (ExecutionException ee) {
			Throwable cause = ee.getCause();
			log.error("Failed to send Kafka event for productId {}: {}", productId, cause.getMessage(), cause);
			// inspect cause to decide: SerializationException, TimeoutException, KafkaException, etc.
			throw new ProductCreationEventKafkaException("Failed to send Kafka event");
		}
		return ProductCreationResponse.builder().productId(productId).build();
	}

}
