package pe.ask.kafka.core.model;

import lombok.Builder;
import lombok.Singular;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable event envelope transmitted as the value of a Kafka record.
 *
 * <p>This model contains business-level information about an event, including
 * its identity, type, origin, timestamps, correlation information and payload.</p>
 *
 * <p>Transport information such as partition, offset and record key is not
 * included in the serialized event body.</p>
 *
 * @param eventId       unique identifier of the event
 * @param topic         Kafka topic where the event will be published
 * @param eventType     logical event type, such as {@code CustomerCreated}
 * @param source        application or component that generated the event
 * @param subject       entity or resource associated with the event
 * @param occurredAt    date and time when the business event occurred
 * @param createdAt     date and time when the envelope was created
 * @param correlationId identifier shared by events from the same business flow
 * @param causationId   identifier of the event or command that caused this event
 * @param contentType   media type used to serialize the payload
 * @param payload       event data
 * @param metadata      additional immutable application-level metadata
 * @param headers       Kafka headers
 * @param <M>           payload type
 *
 * @author Allan Sagastegui
 */
@Builder(toBuilder = true)
public record KafkaMessage<M>(
        UUID eventId,
        String topic,
        String eventType,
        String source,
        String subject,
        Instant occurredAt,
        Instant createdAt,
        String correlationId,
        String causationId,
        String contentType,
        M payload,
        @Singular("metadataEntry")
        Map<String, String> metadata,
        @Singular("header")
        Map<String, String> headers
) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_CONTENT_TYPE = "application/json";

    /**
     * Applies default values and validates mandatory properties.
     */
    public KafkaMessage {
        eventId = Objects.requireNonNullElseGet(
                eventId,
                UUID::randomUUID
        );

        Instant now = Instant.now();

        occurredAt = Objects.requireNonNullElse(
                occurredAt,
                now
        );

        createdAt = Objects.requireNonNullElse(
                createdAt,
                now
        );

        correlationId = Objects.requireNonNullElse(
                correlationId,
                eventId.toString()
        );

        contentType = Objects.requireNonNullElse(
                contentType,
                DEFAULT_CONTENT_TYPE
        );

        metadata = metadata == null
                ? Map.of()
                : Map.copyOf(metadata);

        headers = headers == null
                ? Map.of()
                : Map.copyOf(headers);

        requireText(topic, "topic");
        requireText(eventType, "eventType");
        requireText(source, "source");
    }

    private static void requireText(String value, String property) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    property + " must not be null or blank"
            );
        }
    }
}
