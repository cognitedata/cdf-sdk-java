package com.cognite.client.stream;

import com.cognite.client.RawRows;
import com.cognite.client.dto.RawRow;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 *
 */
@AutoValue
public abstract class RawPublisher {
    protected static final Logger LOG = LoggerFactory.getLogger(RawPublisher.class);

    private AtomicBoolean abortStream = new AtomicBoolean(false);

    private static Builder builder() {
        return new AutoValue_RawPublisher.Builder()
                .setPollingInterval(Duration.ofSeconds(5))
                .setPollingOffset(Duration.ofSeconds(2))
                .setStartTime(Instant.ofEpochMilli(1L))
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
    @Nullable
    abstract Consumer<List<RawRow>> getConsumer();

    void run() throws Exception {
        final String loggingPrefix = "streaming() [" + RandomStringUtils.randomAlphanumeric(6) + "] - ";
        Preconditions.checkNotNull(getConsumer(),
                loggingPrefix + "You must specify a Consumer via withConsumer(Consumer<List<RawRow>>)");
        Preconditions.checkState(getStartTime().isBefore(getEndTime()),
                String.format(loggingPrefix + "Start time must be before end time. Start time: %s. End time: %s",
                        getStartTime(),
                        getEndTime()));
        LOG.info(loggingPrefix + "Setting up streaming read from CDF.Raw: {}.{}",
                getRawDbName(),
                getRawTableName());

        // Set the time range for the first query
        long startRange = getStartTime().toEpochMilli();
        long endRange = Instant.now().minus(getPollingOffset()).toEpochMilli();

        while (Instant.now().isBefore(getEndTime().minus(getPollingOffset())) && !abortStream.get()) {
            endRange = Instant.now().minus(getPollingOffset()).toEpochMilli();

            // Sleep for a polling interval
            try {
                Thread.sleep(getPollingInterval().toMillis());
            } catch (Exception e) {
                LOG.warn(loggingPrefix + "Exception when reading: " + e.toString());
                abortStream.set(true);
            }
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
        abstract Builder setConsumer(Consumer<List<RawRow>> value);

        abstract RawPublisher build();
    }
}