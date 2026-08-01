package pe.ask.kafka.core.exceptions;

import pe.ask.exception.core.annotation.ExceptionDef;
import pe.ask.exception.core.model.BaseException;

@ExceptionDef(ErrorCatalog.class)
public class KafkaProducerException extends BaseException {
    public KafkaProducerException() {
        super();
    }

    public static Builder<KafkaProducerException> builder() {
        return new Builder<>(KafkaProducerException.class);
    }
}
