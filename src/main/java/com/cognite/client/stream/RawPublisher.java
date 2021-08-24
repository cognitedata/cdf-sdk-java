package com.cognite.client.stream;

import com.cognite.client.RawRows;
import com.google.auto.value.AutoValue;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

/**
 *
 */
@AutoValue
public abstract class RawPublisher {
    protected static final Logger LOG = LoggerFactory.getLogger(RawPublisher.class);

    private static Builder builder() {
        return new AutoValue_RawPublisher.Builder()
                .setPollingInterval(Duration.ofSeconds(5))
                .setPollingOffset(Duration.ofSeconds(2))
                .setStartTime(Instant.MIN)
                .setEndTime(Instant.MAX)
                ;
    }

    public static RawPublisher of(RawRows rawRows,
                                  String rawDbName,
                                  String rawTableName) {
        return RawPublisher.builder()
                .setRawRows(rawRows)
                .setRawDbName(rawDbName)
                .setRawTableName(rawTableName)
                .build();
    }

    abstract Builder toBuilder();

    abstract RawRows getRawRows();
    abstract String getRawDbName();
    abstract String getRawTableName();
    abstract Duration getPollingInterval();
    abstract Duration getPollingOffset();
    abstract Instant getStartTime();
    abstract Instant getEndTime();

    void run() {
        final String loggingPrefix = "streaming() [" + RandomStringUtils.randomAlphanumeric(6) + "] - ";
        LOG.info(loggingPrefix + "Setting up streaming read from CDF.Raw {}.{}",
                getRawDbName(),
                getRawTableName());

        long startRange = getStartTime().toEpochMilli();
        long endRange = getEndTime().toEpochMilli();

        while (Instant.now().isBefore(getEndTime().minus(getPollingOffset()))) {


        }

    }


    @AutoValue.Builder
    abstract static class Builder {
        abstract Builder setRawRows(RawRows value);
        abstract Builder setPollingInterval(Duration value);
        abstract Builder setPollingOffset(Duration value);
        abstract Builder setRawDbName(String value);
        abstract Builder setRawTableName(String value);
        abstract Builder setStartTime(Instant value);
        abstract Builder setEndTime(Instant value);

        abstract RawPublisher build();
    }
}