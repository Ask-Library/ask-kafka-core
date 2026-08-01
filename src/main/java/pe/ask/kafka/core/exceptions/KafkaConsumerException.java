package pe.ask.kafka.core.exceptions;

import pe.ask.exception.core.annotation.ExceptionDef;
import pe.ask.exception.core.model.BaseException;

@ExceptionDef(ErrorCatalog.class)
public class KafkaConsumerException extends BaseException{
    public KafkaConsumerException() {
        super();
    }

    public static BaseException.Builder<KafkaConsumerException> builder() {
        return new BaseException.Builder<>(KafkaConsumerException.class);
    }

}
