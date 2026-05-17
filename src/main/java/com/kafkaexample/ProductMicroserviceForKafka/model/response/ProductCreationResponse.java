package com.kafkaexample.ProductMicroserviceForKafka.model.response;

import jakarta.validation.Valid;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ProductCreationResponse {
    private @Valid String productId;
    private @Valid String name;
}
