package com.cognite.client.stream;

import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @param <T>
 * @param <E>
 */
@AutoValue
public abstract class Publisher<T, E extends Source<T>> {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static <T, E extends Source<T>> Builder<T, E> builder() {
        return new AutoValue_Publisher.Builder<T, E>();
    }

    public static <T, E extends Source<T>> Publisher<T, E> of() {
        return Publisher.<T, E>builder().build();
    }

    @AutoValue.Builder
    abstract static class Builder<T, E extends Source<T>> {
        //abstract Publisher.Builder<T> setInputIterators( value);

        abstract Publisher<T, E> build();
    }
}