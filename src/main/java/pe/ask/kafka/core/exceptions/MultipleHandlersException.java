package pe.ask.kafka.core.exceptions;

import pe.ask.exception.core.annotation.ExceptionDef;
import pe.ask.exception.core.model.BaseException;

@ExceptionDef(ErrorCatalog.class)
public class MultipleHandlersException extends BaseException {
    public MultipleHandlersException() {
        super();
    }

    public static BaseException.Builder<MultipleHandlersException> builder() {
        return new BaseException.Builder<>(MultipleHandlersException.class);
    }

}