/*
 * Copyright (c) 2020 Cognite AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognite.client;

import com.cognite.client.config.AuthConfig;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.util.Partition;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * This class represents the Cognite raw databases api endpoint.
 *
 * It provides methods for interacting with the Raw service.
 */
@AutoValue
public abstract class RawDatabases extends ApiBase {

    private static Builder builder() {
        return new AutoValue_RawDatabases.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(RawDatabases.class);

    /**
     * Constructs a new {@link RawDatabases} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static RawDatabases of(CogniteClient client) {
        return RawDatabases.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all database names.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<String> listResults = new ArrayList<>();
     *     client.raw()
     *           .databases()
     *           .list()
     *           .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Raw/operation/getDBs">API Reference - List databases</a>
     *
     * @see CogniteClient
     * @see CogniteClient#raw()
     * @see Raw#databases()
     *
     * @return an {@link Iterator} to page through the db names.
     * @throws Exception
     */
    public Iterator<List<String>> list() throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ResultFutureIterator<String> futureIterator =
                connector.readRawDbNames(getClient().buildAuthConfig());

        return AdapterIterator.of(FanOutIterator.of(ImmutableList.of(futureIterator)), this::parseName);
    }

    /**
     * Creates Raw databases.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     client.raw()
     *           .databases()
     *           .create(List.of("databaseName"));
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Raw/operation/createDBs">API Reference - Create databases</a>
     *
     * @see CogniteClient
     * @see CogniteClient#raw()
     * @see Raw#databases()
     *
     * @param databases The databases to create.
     * @return The created table names.
     * @throws Exception
     */
    public List<String> create(List<String> databases) throws Exception {
        String loggingPrefix = "create() - ";
        Instant startInstant = Instant.now();
        LOG.info(loggingPrefix + "Received {} databases to create.",
                databases.size());

        List<String> deduplicated = new ArrayList<>(new HashSet<>(databases));

        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeRawDbNames();

        List<List<String>> batches = Partition.ofSize(deduplicated, 100);
        for (List<String> batch : batches) {
            List<Map<String, Object>> items = new ArrayList<>();
            for (String table : batch) {
                items.add(ImmutableMap.of("name", table));
            }
            Request request = addAuthInfo(Request.create()
                    .withItems(items));
            ResponseItems<String> response = createItemWriter.writeItems(request);
            if (!response.isSuccessful()) {
                throw new Exception(String.format(loggingPrefix + "Create database request failed: %s",
                        response.getResponseBodyAsString()));
            }
        }

        LOG.info(loggingPrefix + "Successfully created {} databases. Duration: {}.",
                databases.size(),
                Duration.between(startInstant, Instant.now()));

        return deduplicated;
    }

    /**
     * Deletes a set of Raw databases. The databases must be empty.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<String> deleteItemsResults =
     *         client.raw()
     *               .databases()
     *               .delete(List.of("databaseName"));
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Raw/operation/deleteDBs">API Reference - Delete databases</a>
     *
     * @see #delete(List, boolean)
     * @see CogniteClient
     * @see CogniteClient#raw()
     * @see Raw#databases()
     *
     * @param databases The Raw database to delete.
     * @return The deleted databases.
     * @throws Exception
     */
    public List<String> delete(List<String> databases) throws Exception {
        return delete(databases, false);
    }

    /**
     * Deletes a set of Raw databases. Allows to recursively delete the databases' tables in the same operation.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<String> deleteItemsResults =
     *         client.raw()
     *               .databases()
     *               .delete(List.of("databaseName"), true);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Raw/operation/deleteDBs">API Reference - Delete databases</a>
     *
     * @see CogniteClient
     * @see CogniteClient#raw()
     * @see Raw#databases()
     *
     * @param databases The Raw database to delete.
     * @param recursive Set to true to automatically delete the tables in the databases.
     * @return The deleted databases.
     * @throws Exception
     */
    public List<String> delete(List<String> databases, boolean recursive) throws Exception {
        String loggingPrefix = "delete() - ";
        Instant startInstant = Instant.now();
        LOG.info(loggingPrefix + "Received {} databases to delete.",
                databases.size());

        List<String> deduplicated = new ArrayList<>(new HashSet<>(databases));

        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteRawDbNames();

        List<List<String>> batches = Partition.ofSize(deduplicated, 100);
        for (List<String> batch : batches) {
            List<Map<String, Object>> items = new ArrayList<>();
            for (String table : batch) {
                items.add(ImmutableMap.of("name", table));
            }
            Request request = addAuthInfo(Request.create()
                    .withItems(items)
                    .withRootParameter("recursive", recursive));
            ResponseItems<String> response = deleteItemWriter.writeItems(request);
            if (!response.isSuccessful()) {
                throw new Exception(String.format(loggingPrefix + "Delete database request failed: %s",
                        response.getResponseBodyAsString()));
            }
        }

        LOG.info(loggingPrefix + "Successfully deleted {} databases. Duration: {}.",
                databases.size(),
                Duration.between(startInstant, Instant.now()));

        return deduplicated;
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract RawDatabases build();
    }
}
