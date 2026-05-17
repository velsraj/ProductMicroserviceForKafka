package com.kafkaexample.ProductMicroserviceForKafka.exception.handler;

import com.kafkaexample.ProductMicroserviceForKafka.exception.ProductCreationEventKafkaException;
import com.kafkaexample.ProductMicroserviceForKafka.model.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.time.LocalDateTime;

@ControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(ProductCreationEventKafkaException.class)
    public ResponseEntity<ErrorResponse> handleKafkaSendException(ProductCreationEventKafkaException kafkaSendException) {
        ErrorResponse errorResponse = ErrorResponse
                .builder()
                .errorCode(HttpStatus.BAD_REQUEST)
                .errorMessage(kafkaSendException.getMessage())
                .errorTime(LocalDateTime.now())
                .apiPath("/v1/products")
                .build();
        return new ResponseEntity<>(errorResponse,HttpStatus.BAD_REQUEST);
    }

}
