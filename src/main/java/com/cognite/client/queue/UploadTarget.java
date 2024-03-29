package com.cognite.client.queue;

import java.util.List;

/**
 * An interface for API endpoints that supports uploading a resource type T.
 *
 * @param <T> The type of the resource to upsert to CDF.
 * @param <R> The return type of the upload operation.
 */
public interface UploadTarget<T, R> {

    /**
     * Upserts a collection of objects to Cognite Data Fusion.
     *
     * <p>
     * If it is a new object (based on {@code id / externalId}, then it will be created.
     * <p>
     * If the object already exists in Cognite Data Fusion, it will be updated. The update behavior
     * is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * @param objects the objects to upsert to CDF
     * @return a list of the confirmed upserted objects.
     */
    public List<R> upload(List<T> objects) throws Exception;
}
