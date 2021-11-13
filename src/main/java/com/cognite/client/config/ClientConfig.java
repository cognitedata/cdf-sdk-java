package com.cognite.client.config;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;

import java.io.Serializable;

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

    private static Builder builder() {
        return new AutoValue_ClientConfig.Builder()
                .setSdkIdentifier(SDK_IDENTIFIER)
                .setAppIdentifier(DEFAULT_APP_IDENTIFIER)
                .setSessionIdentifier(DEFAULT_SESSION_IDENTIFIER)
                .setMaxRetries(DEFAULT_RETRIES)
                .setNoWorkers(DEFAULT_CPU_THREADS)
                .setNoListPartitions(DEFAULT_LIST_PARTITIONS)
                .setUpsertMode(DEFAULT_UPSERT_MODE)
                .setEntityMatchingMaxBatchSize(DEFAULT_ENTITY_MATCHING_MAX_BATCH_SIZE);
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
    public abstract int getNoListPartitions();
    public abstract UpsertMode getUpsertMode();
    public abstract int getEntityMatchingMaxBatchSize();

    /**
     * Set the app identifier. The identifier is encoded in the api calls to the Cognite instance and can be
     * used for tracing and statistics.
     *
     * @param identifier the application identifier
     * @return the {@link ClientConfig} with the setting applied
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
     * @param identifier the session identifier
     * @return the {@link ClientConfig} with the setting applied
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
     * @param retries the {@link ClientConfig} with the setting applied
     * @return the {@link ClientConfig} with the setting applied
     */
    public ClientConfig withMaxRetries(int retries) {
        Preconditions.checkArgument(retries <= MAX_RETRIES && retries >= MIN_RETRIES,
                String.format("The max number of retries must be between %d and %d", MIN_RETRIES, MAX_RETRIES));
        return toBuilder().setMaxRetries(retries).build();
    }

    /**
     * Specifies the maximum number of workers to use for the Cognite API requests. The default setting is
     * eight workers per (virtual) CPU core.
     *
     * @param noWorkers max number of workers
     * @return the {@link ClientConfig} with the setting applied
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
     * @param noPartitions the number of partitions for list requests
     * @return the {@link ClientConfig} with the setting applied
     */
    public ClientConfig withNoListPartitions(int noPartitions) {
        return toBuilder().setNoListPartitions(noPartitions).build();
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
     * @param mode the upsert mode
     * @return the {@link ClientConfig} with the setting applied
     */
    public ClientConfig withUpsertMode(UpsertMode mode) {
        return toBuilder().setUpsertMode(mode).build();
    }

    /**
     * Sets the max batch size when executing entity matching operations.
     * @param value
     * @return
     */
    public ClientConfig withEntityMatchingMaxBatchSize(int value) {
        checkArgument(value >= MIN_ENTITY_MATCHING_MAX_BATCH_SIZE
                        && value <= MAX_ENTITY_MATCHING_MAX_BATCH_SIZE,
                String.format("Max context batch size must be between %d and %d", MIN_ENTITY_MATCHING_MAX_BATCH_SIZE,
                        MAX_ENTITY_MATCHING_MAX_BATCH_SIZE));

        return toBuilder().setEntityMatchingMaxBatchSize(value).build();
    }

    @AutoValue.Builder
    static abstract class Builder {
        abstract Builder setSdkIdentifier(String value);
        abstract Builder setAppIdentifier(String value);
        abstract Builder setSessionIdentifier(String value);
        abstract Builder setMaxRetries(int value);
        abstract Builder setNoWorkers(int value);
        abstract Builder setNoListPartitions(int value);
        abstract Builder setUpsertMode(UpsertMode value);
        abstract Builder setEntityMatchingMaxBatchSize(int value);

        abstract ClientConfig build();
    }
}
