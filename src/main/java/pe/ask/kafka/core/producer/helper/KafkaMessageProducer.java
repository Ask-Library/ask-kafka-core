package pe.ask.kafka.core.producer.helper;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import pe.ask.kafka.core.exceptions.KafkaProducerException;
import pe.ask.kafka.core.model.KafkaMessage;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@RequiredArgsConstructor
public abstract class KafkaMessageProducer {

    private final KafkaSender<String, String> reactorKafkaSender;
    private final ObjectMapper objectMapper;

    /**
     * Serializes and sends a Kafka message.
     *
     * @param message message envelope to send
     * @param <M>     payload type
     * @return completion signal
     */
    public <M> Mono<Void> send(KafkaMessage<M> message) {
        return Mono.justOrEmpty(message)
                .switchIfEmpty(Mono.error(KafkaProducerException::new))
                .flatMap(this::createSenderRecord)
                .transformDeferred(reactorKafkaSender::send)
                .single()
                .flatMap(this::validateResult);
    }

    private <M> Mono<SenderRecord<String, String, UUID>> createSenderRecord(
            KafkaMessage<M> message
    ) {
        return serialize(message)
                .map(json -> createProducerRecord(message, json))
                .map(producerRecord ->
                        SenderRecord.create(
                                producerRecord,
                                message.eventId()
                        )
                );
    }

    private <M> Mono<String> serialize(KafkaMessage<M> message) {
        return Mono.fromCallable(
                () -> objectMapper.writeValueAsString(message)
        );
    }

    private <M> ProducerRecord<String, String> createProducerRecord(
            KafkaMessage<M> message,
            String serializedMessage
    ) {
        return new ProducerRecord<>(
                message.topic(),
                null,
                null,
                message.eventId().toString(),
                serializedMessage,
                createHeaders(message)
        );
    }

    private <M> RecordHeaders createHeaders(KafkaMessage<M> message) {
        Iterable<Header> headers = message.headers()
                .entrySet()
                .stream()
                .map(entry -> new RecordHeader(
                        entry.getKey(),
                        entry.getValue().getBytes(StandardCharsets.UTF_8)
                ))
                .map(Header.class::cast)
                .toList();

        return new RecordHeaders(headers);
    }

    private Mono<Void> validateResult(SenderResult<UUID> result) {
        return Mono.justOrEmpty(result.exception())
                .flatMap(Mono::error);
    }
}
