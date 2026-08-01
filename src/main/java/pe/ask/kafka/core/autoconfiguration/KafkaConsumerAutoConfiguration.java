package pe.ask.kafka.core.autoconfiguration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import pe.ask.kafka.core.consumer.dispatcher.KafkaGlobalDispatcher;
import pe.ask.kafka.core.consumer.helper.ReactiveKafkaConsumerOperations;
import pe.ask.kafka.core.consumer.properties.KafkaConsumerProperties;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

@AutoConfiguration
@ConditionalOnClass({
        KafkaReceiver.class,
        ReceiverOptions.class
})
@EnableConfigurationProperties(KafkaConsumerProperties.class)
@ConditionalOnProperty(
        prefix = "pe.ask.kafka",
        name = "type",
        havingValue = "consumer"
)
public class KafkaConsumerAutoConfiguration {

    /**
     * Creates the Reactor Kafka receiver options.
     *
     * @param properties consumer configuration properties
     * @return configured receiver options
     */
    @Bean
    @ConditionalOnMissingBean
    public ReceiverOptions<String, String> kafkaReceiverOptions(
            KafkaConsumerProperties properties
    ) {
        KafkaConsumerProperties.ConsumerProperties consumer =
                properties.consumer();

        Map<String, Object> kafkaProperties = Map.ofEntries(
                Map.entry(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        consumer.bootstrapServers()
                ),
                Map.entry(
                        ConsumerConfig.GROUP_ID_CONFIG,
                        consumer.groupId()
                ),
                Map.entry(
                        ConsumerConfig.CLIENT_ID_CONFIG,
                        consumer.clientId()
                ),
                Map.entry(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        consumer.keyDeserializer()
                ),
                Map.entry(
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        consumer.valueDeserializer()
                ),
                Map.entry(
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        consumer.autoOffsetReset()
                ),
                Map.entry(
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                        consumer.enableAutoCommit()
                ),
                Map.entry(
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                        consumer.maxPollRecords()
                ),
                Map.entry(
                        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                        consumer.maxPollInterval().toMillis()
                )
        );

        return ReceiverOptions
                .<String, String>create(kafkaProperties)
                .subscription(consumer.topics())
                .pollTimeout(consumer.pollTimeout())
                .commitInterval(consumer.commitInterval())
                .commitBatchSize(consumer.commitBatchSize())
                .maxCommitAttempts(consumer.maxCommitAttempts())
                .commitRetryInterval(consumer.commitRetryInterval())
                .maxDeferredCommits(consumer.maxDeferredCommits())
                .maxDelayRebalance(consumer.maxDelayRebalance())
                .closeTimeout(consumer.closeTimeout());
    }

    /**
     * Creates the Kafka receiver used by the global dispatcher.
     *
     * @param receiverOptions configured receiver options
     * @return Kafka receiver
     */
    @Bean
    @ConditionalOnMissingBean
    public KafkaReceiver<String, String> kafkaReceiver(
            ReceiverOptions<String, String> receiverOptions
    ) {
        return KafkaReceiver.create(receiverOptions);
    }

    /**
     * Creates the dispatcher responsible for routing records to their
     * corresponding topic handlers.
     *
     * @param kafkaReceiver Kafka receiver
     * @param handlers      registered consumer handlers
     * @return global Kafka dispatcher
     */
    @Bean
    @ConditionalOnBean(ReactiveKafkaConsumerOperations.class)
    @ConditionalOnMissingBean
    public KafkaGlobalDispatcher kafkaGlobalDispatcher(
            KafkaReceiver<String, String> kafkaReceiver,
            List<ReactiveKafkaConsumerOperations<?>> handlers
    ) {
        return new KafkaGlobalDispatcher(
                kafkaReceiver,
                handlers
        );
    }
}
