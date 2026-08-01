package pe.ask.kafka.core.consumer.helper;

import lombok.RequiredArgsConstructor;
import pe.ask.kafka.core.exceptions.KafkaConsumerException;
import pe.ask.kafka.core.model.KafkaMessage;
import reactor.core.publisher.Mono;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.ObjectMapper;

@RequiredArgsConstructor
public abstract class ReactiveKafkaConsumerOperations<M> {

    private final ObjectMapper objectMapper;

    public final Mono<Void> handleMessage(String jsonMessage) {
        return Mono.justOrEmpty(jsonMessage)
                .filter(json -> !json.isBlank())
                .switchIfEmpty(Mono.error(KafkaConsumerException::new))
                .flatMap(this::deserialize)
                .flatMap(this::processRecord);
    }

    private Mono<KafkaMessage<M>> deserialize(
            String jsonMessage
    ) {
        return Mono.fromCallable(() ->
                objectMapper.<KafkaMessage<M>>readValue(
                        jsonMessage,
                        kafkaMessageType()
                )
        );
    }

    private JavaType kafkaMessageType() {
        return objectMapper
                .getTypeFactory()
                .constructParametricType(
                        KafkaMessage.class,
                        getPayloadClass()
                );
    }

    public abstract String getTargetTopic();

    protected abstract Class<M> getPayloadClass();

    protected abstract Mono<Void> processRecord(
            KafkaMessage<M> message
    );
}
