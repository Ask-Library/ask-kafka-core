package pe.ask.kafka.core.consumer.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.List;

@Validated
@ConfigurationProperties(prefix = "pe.ask.kafka")
public record KafkaConsumerProperties(

        @DefaultValue("consumer")
        String type,

        @Valid
        @NotNull
        @DefaultValue
        ConsumerProperties consumer
) {

        public record ConsumerProperties(

                @NotBlank
                String bootstrapServers,

                @NotBlank
                String groupId,

                @NotEmpty
                List<@NotBlank String> topics,

                @DefaultValue("ask-kafka-consumer")
                String clientId,

                @DefaultValue(
                        "org.apache.kafka.common.serialization.StringDeserializer"
                )
                Class<? extends Deserializer<?>> keyDeserializer,

                @DefaultValue(
                        "org.apache.kafka.common.serialization.StringDeserializer"
                )
                Class<? extends Deserializer<?>> valueDeserializer,

                @DefaultValue("latest")
                String autoOffsetReset,

                @DefaultValue("false")
                boolean enableAutoCommit,

                @DefaultValue("500")
                @Positive
                int maxPollRecords,

                @DefaultValue("5m")
                @NotNull
                Duration maxPollInterval,

                @DefaultValue("1s")
                @NotNull
                Duration pollTimeout,

                @DefaultValue("5s")
                @NotNull
                Duration commitInterval,

                @DefaultValue("0")
                @PositiveOrZero
                int commitBatchSize,

                @DefaultValue("3")
                @Positive
                int maxCommitAttempts,

                @DefaultValue("500ms")
                @NotNull
                Duration commitRetryInterval,

                @DefaultValue("0")
                @PositiveOrZero
                int maxDeferredCommits,

                @DefaultValue("60s")
                @NotNull
                Duration maxDelayRebalance,

                @DefaultValue("30s")
                @NotNull
                Duration closeTimeout
        ) {

                public ConsumerProperties {
                        requirePositiveDuration(
                                maxPollInterval,
                                "maxPollInterval"
                        );

                        requirePositiveDuration(
                                pollTimeout,
                                "pollTimeout"
                        );

                        requirePositiveDuration(
                                commitRetryInterval,
                                "commitRetryInterval"
                        );

                        requirePositiveDuration(
                                maxDelayRebalance,
                                "maxDelayRebalance"
                        );

                        requirePositiveDuration(
                                closeTimeout,
                                "closeTimeout"
                        );

                        requireNonNegativeDuration(
                                commitInterval
                        );

                        if (maxDelayRebalance.compareTo(maxPollInterval) >= 0) {
                                throw new IllegalArgumentException(
                                        "maxDelayRebalance must be less than maxPollInterval"
                                );
                        }
                }

                private static void requirePositiveDuration(
                        Duration duration,
                        String property
                ) {
                        if (duration == null || duration.isZero() || duration.isNegative()) {
                                throw new IllegalArgumentException(
                                        property + " must be greater than zero"
                                );
                        }
                }

                private static void requireNonNegativeDuration(
                        Duration duration
                ) {
                        if (duration == null || duration.isNegative()) {
                                throw new IllegalArgumentException(
                                        "commitInterval" + " must not be negative"
                                );
                        }
                }
        }
}