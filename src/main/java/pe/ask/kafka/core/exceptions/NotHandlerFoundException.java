package pe.ask.kafka.core.exceptions;

import pe.ask.exception.core.annotation.ExceptionDef;
import pe.ask.exception.core.model.BaseException;

@ExceptionDef(ErrorCatalog.class)
public class NotHandlerFoundException extends BaseException {
    public NotHandlerFoundException() {
        super();
    }

    public static BaseException.Builder<NotHandlerFoundException> builder() {
        return new BaseException.Builder<>(NotHandlerFoundException.class);
    }

}
