package com.kafkaexample.ProductMicroserviceForKafka.model.request;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class ProductCreationRequest {

	private @Valid String title;

	@NotNull
	private @Valid BigDecimal price;

	private @Valid Integer quantity;

}
