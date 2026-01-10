package com.cadosfrit.sensor.event.service.exception;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.time.Instant;

@Slf4j
@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        String typeName = (ex.getRequiredType() != null) ? ex.getRequiredType().getSimpleName() : "Unknown";

        String errorMsg = String.format("Invalid value for parameter '%s'. Expected type: %s",
                ex.getName(), typeName);

        log.warn("Input validation failed: {}", errorMsg);
        return buildResponse(HttpStatus.BAD_REQUEST, errorMsg);
    }


    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ErrorResponse> handleMissingParams(MissingServletRequestParameterException ex) {
        String errorMsg = String.format("Missing required parameter: '%s'", ex.getParameterName());

        log.warn("Missing parameter: {}", errorMsg);
        return buildResponse(HttpStatus.BAD_REQUEST, errorMsg);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleMalformedJson(HttpMessageNotReadableException ex) {
        log.warn("Malformed JSON request: {}", ex.getMessage());
        return buildResponse(HttpStatus.BAD_REQUEST, "Malformed JSON request. Please check your request body.");
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFoundException(ResourceNotFoundException ex) {
        log.warn("Resource not found: {}", ex.getMessage());
        return buildResponse(HttpStatus.NOT_FOUND, ex.getMessage());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneralException(Exception ex) {
        log.error("Unexpected system error: ", ex); // Log full stack trace here
        return buildResponse(HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred. Please contact support.");
    }

    private ResponseEntity<ErrorResponse> buildResponse(HttpStatus status, String message) {
        ErrorResponse response = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(status.value())
                .error(status.getReasonPhrase())
                .message(message)
                .build();
        return new ResponseEntity<>(response, status);
    }

    @Data
    @Builder
    public static class ErrorResponse {
        private Instant timestamp;
        private int status;
        private String error;
        private String message;
    }
}