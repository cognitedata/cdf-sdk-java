package com.cognite.client.config;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;

import static com.cognite.client.servicesV1.ConnectorConstants.DEFAULT_ASYNC_API_JOB_TIMEOUT;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class captures the client configuration parameters.
 */
@AutoValue
public abstract class ClientConfig implements Serializable {
    /*
    API request identifiers
     */
    private final static String SDK_IDENTIFIER = "cognite-java-sdk-1.x.x";
    private final static String DEFAULT_APP_IDENTIFIER = "cognite-java-sdk";
    private final static String DEFAULT_SESSION_IDENTIFIER = "cognite-java-sdk";


    private final static UpsertMode DEFAULT_UPSERT_MODE = UpsertMode.UPDATE;
    private final static int DEFAULT_LIST_PARTITIONS = 8;

    // Thread pool capacity
    private final static int DEFAULT_CPU_THREADS = 8;
    private final static int DEFAULT_CPU_THREADS_TS = 16;

    // Connection retries
    private static final int DEFAULT_RETRIES = 5;
    private static final int MAX_RETRIES = 20;
    private static final int MIN_RETRIES = 1;

    // Max batch size when writing to Raw
    private static final int DEFAULT_WRITE_RAW_MAX_BATCH_SIZE = 2000;
    private static final int MIN_WRITE_RAW_MAX_BATCH_SIZE = 1;
    private static final int MAX_WRITE_RAW_MAX_BATCH_SIZE = 10000;

    // Max batch size for context operations
    private static final int DEFAULT_ENTITY_MATCHING_MAX_BATCH_SIZE = 1000;
    private static final int MIN_ENTITY_MATCHING_MAX_BATCH_SIZE = 1;
    private static final int MAX_ENTITY_MATCHING_MAX_BATCH_SIZE = 20000;

    // Timeouts for async api jobs (i.e. the context api)
    private static final Duration MIN_ASYNC_API_JOB_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration MAX_ASYNC_API_JOB_TIMEOUT = Duration.ofHours(24);

    private static Builder builder() {
        return new AutoValue_ClientConfig.Builder()
                .setSdkIdentifier(SDK_IDENTIFIER)
                .setAppIdentifier(DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(DEFAULT_SESSION_IDENTIFIER)
                .setMaxRetries(DEFAULT_RETRIES)
                .setNoWorkers(DEFAULT_CPU_THREADS)
                .setNoTsWorkers(DEFAULT_CPU_THREADS_TS)
                .setNoListPartitions(DEFAULT_LIST_PARTITIONS)
                .setUpsertMode(DEFAULT_UPSERT_MODE)
                .setEntityMatchingMaxBatchSize(DEFAULT_ENTITY_MATCHING_MAX_BATCH_SIZE)
                .setAsyncApiJobTimeout(DEFAULT_ASYNC_API_JOB_TIMEOUT)
                .setExperimental(FeatureFlag.create());
    }

    /**
     * Returns a {@link ClientConfig} object with default settings.
     *
     * @return the {@link ClientConfig} object.
     */
    public static ClientConfig create() {
        return ClientConfig.builder().build();
    }

    abstract Builder toBuilder();

    public abstract String getSdkIdentifier();
    public abstract String getAppIdentifier();
    public abstract String getSessionIdentifier();
    public abstract int getMaxRetries();
    public abstract int getNoWorkers();
    public abstract int getNoTsWorkers();
    public abstract int getNoListPartitions();
    public abstract UpsertMode getUpsertMode();
    public abstract int getEntityMatchingMaxBatchSize();
    public abstract Duration getAsyncApiJobTimeout();
    @Nullable public abstract ProxyConfig getProxyConfig();
    public abstract FeatureFlag getExperimental();

    /**
     * Set the app identifier. The identifier is encoded in the api calls to the Cognite instance and can be
     * used for tracing and statistics.
     *
     * @param identifier the application identifier.
     * @return the {@link ClientConfig} with the setting applied.
     */
    public ClientConfig withAppIdentifier(String identifier) {
        Preconditions.checkNotNull(identifier, "Identifier cannot be null");
        Preconditions.checkArgument(!identifier.isEmpty(), "Identifier cannot be an empty string.");
        return toBuilder().setAppIdentifier(identifier).build();
    }

    /**
     * Set the session identifier. The identifier is encoded in the api calls to the Cognite instance and can be
     * used for tracing and statistics.
     *
     * @param identifier the session identifier.
     * @return the {@link ClientConfig} with the setting applied.
     */
    public ClientConfig withSessionIdentifier(String identifier) {
        Preconditions.checkNotNull(identifier, "Identifier cannot be null");
        Preconditions.checkArgument(!identifier.isEmpty(), "Identifier cannot be an empty string.");
        return toBuilder().setSessionIdentifier(identifier).build();
    }

    /**
     * Sets the maximum number of retries when sending requests to the Cognite API.
     *
     * The default setting is 5. This should be sufficient for most scenarios.
     *
     * @param retries the maximum number of retries before failing a request.
     * @return the {@link ClientConfig} with the setting applied.
     */
    public ClientConfig withMaxRetries(int retries) {
        Preconditions.checkArgument(retries <= MAX_RETRIES && retries >= MIN_RETRIES,
                String.format("The max number of retries must be between %d and %d", MIN_RETRIES, MAX_RETRIES));
        return toBuilder().setMaxRetries(retries).build();
    }

    /**
     * Specifies the maximum number of workers to use for the Cognite API requests. The default setting is
     * eight workers.
     *
     * @param noWorkers max number of workers.
     * @return the {@link ClientConfig} with the setting applied.
     */
    public ClientConfig withNoWorkers(int noWorkers) {
        return toBuilder().setNoWorkers(noWorkers).build();
    }

    /**
     * Specifies the number of partitions to use for (read) list requests. This represents the number
     * of parallel streams to use for reading the results of list requests.
     *
     * The default setting is eight partitions.
     *
     * @param noPartitions the number of partitions for list requests.
     * @return the {@link ClientConfig} with the setting applied.
     */
    public ClientConfig withNoListPartitions(int noPartitions) {
        return toBuilder().setNoListPartitions(noPartitions).build();
    }

    /**
     * Specifies the maximum number of workers to use for the Cognite API requests towards the time series service.
     * This is a high-capacity service optimized for a higher number of workers than the other API services. The default
     * setting is 16 workers.
     *
     * @param noTsWorkers max number of workers.
     * @return the {@link ClientConfig} with the setting applied.
     */
    public ClientConfig withNoTsWorkers(int noTsWorkers) {
        return toBuilder().setNoTsWorkers(noTsWorkers).build();
    }

    /**
     * Sets the upsert mode.
     *
     * When the data object to write does not exist, the writer will always create it. But, if the
     * object already exist, the writer can update the the existing object in one of two ways: update or replace.
     *
     * <code>UpsertMode.UPDATE</code> will update the provided fields in the target object--all other fields will remain
     * unchanged.
     *
     * <code>UpsertMode.REPLACE</code> will replace the entire target object with the provided fields
     * (<code>id</code> and <code>externalId</code> will remain unchanged).
     *
     * @param mode the upsert mode.
     * @return the {@link ClientConfig} with the setting applied
     */
    public ClientConfig withUpsertMode(UpsertMode mode) {
        return toBuilder().setUpsertMode(mode).build();
    }

    /**
     * Sets the max batch size when executing entity matching operations.
     *
     * @param batchSize the max batch size.
     * @return the {@link ClientConfig} with the setting applied.
     */
    public ClientConfig withEntityMatchingMaxBatchSize(int batchSize) {
        checkArgument(batchSize >= MIN_ENTITY_MATCHING_MAX_BATCH_SIZE
                        && batchSize <= MAX_ENTITY_MATCHING_MAX_BATCH_SIZE,
                String.format("Max context batch size must be between %d and %d", MIN_ENTITY_MATCHING_MAX_BATCH_SIZE,
                        MAX_ENTITY_MATCHING_MAX_BATCH_SIZE));

        return toBuilder().setEntityMatchingMaxBatchSize(batchSize).build();
    }

    /**
     * Sets the timeout for waiting for async api jobs to finish. Async api jobs includes the CDF context api endpoints
     * like entity matching and engineering diagram parsing.
     *
     * The default timeout is 20 minutes.
     *
     * @param timeout The async timeout expressed as {@link Duration}.
     * @return the {@link ClientConfig} with the setting applied.
     */
    public ClientConfig withAsyncApiJobTimeout(Duration timeout) {
        checkArgument(timeout.compareTo(MIN_ASYNC_API_JOB_TIMEOUT) >= 0
                        && timeout.compareTo(MAX_ASYNC_API_JOB_TIMEOUT) <= 0,
                String.format("Async job timeout must be between %s and %s", MIN_ASYNC_API_JOB_TIMEOUT,
                        MAX_ASYNC_API_JOB_TIMEOUT));

        return toBuilder().setAsyncApiJobTimeout(timeout).build();
    }

    /**
     * Specifies the proxy server configuration.
     *
     * The default setting is no proxy server configuration. If you need to specify a proxy server configuration,
     * you supply a {@link ProxyConfig} object with the appropriate specification.
     *
     * @see ProxyConfig
     * @param proxyConfig the proxy server configuration to use.
     * @return the {@link ClientConfig} with the setting applied.
     */
    public ClientConfig withProxyConfig(ProxyConfig proxyConfig) {
        return toBuilder().setProxyConfig(proxyConfig).build();
    }

    /**
     * Enable/disable experimental features.
     *
     * Experimental features are not fully tested and does not offer future compatibility guarantees.
     *
     * @param experimentalFeatures the experimental feature flags.
     * @return the {@link ClientConfig} with the setting applied.
     */
    public ClientConfig withExperimental(FeatureFlag experimentalFeatures)  {
        return toBuilder().setExperimental(experimentalFeatures).build();
    }

    @AutoValue
    public static abstract class FeatureFlag {
        private static final boolean DEFAULT_ENABLE_DATA_POINTS_CURSOR = true;
        private static Builder builder() {
            return new AutoValue_ClientConfig_FeatureFlag.Builder()
                    .setDataPointsCursorEnabled(DEFAULT_ENABLE_DATA_POINTS_CURSOR);
        }

        public static FeatureFlag create() {
            return FeatureFlag.builder().build();
        }

        abstract Builder toBuilder();

        public abstract boolean isDataPointsCursorEnabled();

        public FeatureFlag enableDataPointsCursor(boolean enable) {
            return toBuilder().setDataPointsCursorEnabled(enable).build();
        }

        @AutoValue.Builder
        public static abstract class Builder {
            abstract Builder setDataPointsCursorEnabled(boolean value);

            abstract FeatureFlag build();
        }
    }

    @AutoValue.Builder
    static abstract class Builder {
        abstract Builder setSdkIdentifier(String value);
        abstract Builder setAppIdentifier(String value);
        abstract Builder setSessionIdentifier(String value);
        abstract Builder setMaxRetries(int value);
        abstract Builder setNoWorkers(int value);
        abstract Builder setNoTsWorkers(int value);
        abstract Builder setNoListPartitions(int value);
        abstract Builder setUpsertMode(UpsertMode value);
        abstract Builder setEntityMatchingMaxBatchSize(int value);
        abstract Builder setAsyncApiJobTimeout(Duration value);
        abstract Builder setProxyConfig(ProxyConfig value);
        abstract Builder setExperimental(FeatureFlag value);

        abstract ClientConfig build();
    }
}
