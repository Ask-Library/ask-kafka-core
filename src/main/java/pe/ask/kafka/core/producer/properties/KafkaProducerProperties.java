package pe.ask.kafka.core.producer.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "pe.ask.kafka")
public record KafkaProducerProperties(
        @DefaultValue("producer")
        String type,

        @Valid
        @NotNull
        ProducerProperties producer
) {

    public record ProducerProperties(
            @NotBlank
            String bootstrapServers,

            @DefaultValue("org.apache.kafka.common.serialization.StringSerializer")
            Class<? extends Serializer<?>> keySerializer,

            @DefaultValue("org.apache.kafka.common.serialization.StringSerializer")
            Class<? extends Serializer<?>> valueSerializer,

            @DefaultValue("ask-kafka-producer")
            String clientId,

            @DefaultValue("all")
            String acknowledgments,

            @DefaultValue("true")
            boolean enableIdempotence,

            @DefaultValue("3")
            @PositiveOrZero
            int retries,

            @DefaultValue("none")
            String compressionType,

            @DefaultValue("16384")
            @Positive
            int batchSize,

            @DefaultValue("5ms")
            Duration linger,

            @DefaultValue("256")
            @Positive
            int maxInFlight,

            @DefaultValue("true")
            boolean stopOnError,

            @DefaultValue("30s")
            Duration closeTimeout
    ) {
    }
}