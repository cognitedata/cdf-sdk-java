package com.cognite.client.stream;

import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@AutoValue
public abstract class RawPublisher {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static Builder builder() {
        return new AutoValue_RawPublisher.Builder();
    }

    public static RawPublisher of() {
        return RawPublisher.builder().build();
    }

    void run() {
        Double aDouble = new Double(234308D);
        aDouble.intValue();
    }


    @AutoValue.Builder
    abstract static class Builder {
        //abstract Publisher.Builder<T> setInputIterators( value);

        abstract RawPublisher build();
    }
}