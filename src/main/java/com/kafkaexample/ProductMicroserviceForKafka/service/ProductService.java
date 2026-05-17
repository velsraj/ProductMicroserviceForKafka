package com.kafkaexample.ProductMicroserviceForKafka.service;


import com.kafkaexample.ProductMicroserviceForKafka.model.request.ProductCreationRequest;
import com.kafkaexample.ProductMicroserviceForKafka.model.response.ProductCreationResponse;

public interface ProductService {
	ProductCreationResponse createProduct(ProductCreationRequest productRestModel);
}
