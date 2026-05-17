package com.kafkaexample.ProductMicroserviceForKafka.controller;

import com.kafkaexample.ProductMicroserviceForKafka.exception.ProductCreationEventKafkaException;
import com.kafkaexample.ProductMicroserviceForKafka.model.request.ProductCreationRequest;
import com.kafkaexample.ProductMicroserviceForKafka.model.response.ProductCreationResponse;
import com.kafkaexample.ProductMicroserviceForKafka.service.ProductService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@Slf4j
public class ProductController {

	private final ProductService productService;

	@Operation(tags = "Products", summary = "Create Product", description = "Create a new product and publish an event to Kafka topic",
			responses = {
					@ApiResponse(responseCode = "200", description = "200", content = {
							@Content(mediaType = "application/json", schema = @Schema(implementation = ProductCreationResponse.class))}),
					@ApiResponse(responseCode = "400", description = "400", content = {
							@Content(mediaType = "application/json", schema = @Schema(implementation = ProductCreationEventKafkaException.class))}),

			})
	@PostMapping(produces = "application/json",
			consumes = "application/json",
			path = "/v1/products"
	)
	public ResponseEntity<ProductCreationResponse> createProduct(@Valid
																	 @Parameter(name = "productCreationRequest")
																	 @RequestBody ProductCreationRequest productCreationRequest) {
		ProductCreationResponse productCreationResponse = productService.createProduct(productCreationRequest);
		return ResponseEntity.status(HttpStatus.CREATED).body(productCreationResponse);
	}

}
