package pe.ask.kafka.core.exceptions;

import pe.ask.exception.core.catalog.BaseExceptionCatalog;

import java.util.Map;

public enum ErrorCatalog implements BaseExceptionCatalog {
    KAFKA_PRODUCER(
            "ERR-KAFKA-001",
            "KAFKA_PRODUCER_EXCEPTION",
            "An unexpected error occurred while serializing or publishing the Kafka message.",
            500,
            null
    ),
    KAFKA_CONSUMER(
            "ERR-KAFKA-002",
            "KAFKA_CONSUMER_EXCEPTION",
            "An unexpected error occurred while receiving, deserializing, or processing the Kafka message.",
            500,
            null
    ),
    NOT_HANDLER_FOUND(
            "ERR-KAFKA-003",
            "KAFKA_HANDLER_NOT_FOUND_EXCEPTION",
            "No Kafka message handler is registered for the received topic.",
            500,
            null
    ),
    MULTIPLE_HANDLERS(
            "ERR-KAFKA-004",
            "KAFKA_MULTIPLE_HANDLERS_EXCEPTION",
            "Multiple Kafka message handlers are registered for the same topic.",
            500,
            null
    );


    private final String errorCode;
    private final String exceptionName;
    private final String message;
    private final int status;
    private final Map<String, String> errors;

    ErrorCatalog(String errorCode, String exceptionName, String message, int status, Map<String, String> errors) {
        this.errorCode = errorCode;
        this.exceptionName = exceptionName;
        this.message = message;
        this.status = status;
        this.errors = errors;
    }

    @Override
    public String getErrorCode() {
        return errorCode;
    }

    @Override
    public String getExceptionName() {
        return exceptionName;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public Map<String, String> getErrors() {
        return errors;
    }
}
