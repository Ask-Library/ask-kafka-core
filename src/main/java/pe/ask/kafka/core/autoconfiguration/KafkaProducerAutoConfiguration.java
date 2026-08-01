package pe.ask.kafka.core.autoconfiguration;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import pe.ask.kafka.core.producer.properties.KafkaProducerProperties;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;

@AutoConfiguration
@ConditionalOnClass({
        KafkaSender.class,
        SenderOptions.class
})
@EnableConfigurationProperties(KafkaProducerProperties.class)
@ConditionalOnProperty(
        prefix = "pe.ask.kafka",
        name = "type",
        havingValue = "producer"
)
public class KafkaProducerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SenderOptions<String, String> kafkaSenderOptions(
            KafkaProducerProperties properties
    ) {
        KafkaProducerProperties.ProducerProperties producer =
                properties.producer();

        Map<String, Object> kafkaProperties = Map.ofEntries(
                Map.entry(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        producer.bootstrapServers()
                ),
                Map.entry(
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        producer.keySerializer()
                ),
                Map.entry(
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        producer.valueSerializer()
                ),
                Map.entry(
                        ProducerConfig.CLIENT_ID_CONFIG,
                        producer.clientId()
                ),
                Map.entry(
                        ProducerConfig.ACKS_CONFIG,
                        producer.acknowledgments()
                ),
                Map.entry(
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                        producer.enableIdempotence()
                ),
                Map.entry(
                        ProducerConfig.RETRIES_CONFIG,
                        producer.retries()
                ),
                Map.entry(
                        ProducerConfig.COMPRESSION_TYPE_CONFIG,
                        producer.compressionType()
                ),
                Map.entry(
                        ProducerConfig.BATCH_SIZE_CONFIG,
                        producer.batchSize()
                ),
                Map.entry(
                        ProducerConfig.LINGER_MS_CONFIG,
                        producer.linger().toMillis()
                )
        );

        return SenderOptions.<String, String>create(kafkaProperties)
                .maxInFlight(producer.maxInFlight())
                .stopOnError(producer.stopOnError())
                .closeTimeout(producer.closeTimeout());
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public KafkaSender<String, String> kafkaSender(
            SenderOptions<String, String> kafkaSenderOptions
    ) {
        return KafkaSender.create(kafkaSenderOptions);
    }
}
