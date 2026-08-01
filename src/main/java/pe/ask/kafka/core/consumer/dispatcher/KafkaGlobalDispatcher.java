package pe.ask.kafka.core.consumer.dispatcher;

import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import pe.ask.kafka.core.consumer.helper.ReactiveKafkaConsumerOperations;
import pe.ask.kafka.core.exceptions.MultipleHandlersException;
import pe.ask.kafka.core.exceptions.NotHandlerFoundException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class KafkaGlobalDispatcher {

    private final KafkaReceiver<String, String> kafkaReceiver;
    private final Map<String, ReactiveKafkaConsumerOperations<?>> handlersMap;

    public KafkaGlobalDispatcher(
            KafkaReceiver<String, String> kafkaReceiver,
            List<ReactiveKafkaConsumerOperations<?>> handlers
    ) {
        this.kafkaReceiver = kafkaReceiver;
        this.handlersMap = handlers.stream()
                .collect(Collectors.toUnmodifiableMap(
                        ReactiveKafkaConsumerOperations::getTargetTopic,
                        Function.identity(),
                        KafkaGlobalDispatcher::duplicateHandler
                ));
    }

    /**
     * Starts consuming Kafka messages when the application has started.
     *
     * @return active Kafka consumption pipeline
     */
    @EventListener(ApplicationStartedEvent.class)
    public Flux<Void> listenMessages() {
        return kafkaReceiver
                .receive()
                .groupBy(this::topicPartition)
                .flatMap(partitionRecords ->
                        partitionRecords.concatMap(
                                this::dispatch
                        )
                )
                .retryWhen(retryPolicy());
    }

    private Mono<Void> dispatch(
            ReceiverRecord<String, String> receiverRecord
    ) {
        return findHandler(receiverRecord.topic())
                .flatMap(handler ->
                        handler.handleMessage(receiverRecord.value())
                )
                .then(acknowledge(receiverRecord));
    }

    private Mono<ReactiveKafkaConsumerOperations<?>> findHandler(String topic) {
        return Mono.<ReactiveKafkaConsumerOperations<?>>justOrEmpty(
                        handlersMap.get(topic)
                )
                .switchIfEmpty(
                        Mono.error(() -> NotHandlerFoundException.builder()
                                .withMessage("No Kafka message handler is registered for the received topic: " + topic)
                                .build()
                        )
                );
    }

    private Mono<Void> acknowledge(
            ReceiverRecord<String, String> receiverRecord
    ) {
        return Mono.fromRunnable(
                receiverRecord.receiverOffset()::acknowledge
        );
    }

    private TopicPartition topicPartition(
            ReceiverRecord<String, String> receiverRecord
    ) {
        return receiverRecord
                .receiverOffset()
                .topicPartition();
    }

    private Retry retryPolicy() {
        return Retry.backoff(
                        Long.MAX_VALUE,
                        Duration.ofSeconds(1)
                )
                .maxBackoff(Duration.ofMinutes(1))
                .jitter(0.5);
    }

    private static ReactiveKafkaConsumerOperations<?> duplicateHandler(
            ReactiveKafkaConsumerOperations<?> first,
            ReactiveKafkaConsumerOperations<?> second
    ) {
        throw MultipleHandlersException.builder()
                .withMessage("Multiple Kafka handlers registered for topic: "
                        + first.getTargetTopic())
                .build();
    }
}
