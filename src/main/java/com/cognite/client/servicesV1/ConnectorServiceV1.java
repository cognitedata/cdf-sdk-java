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

package com.cognite.client.servicesV1;

import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import com.cognite.client.CogniteClient;
import com.cognite.client.Request;
import com.cognite.client.config.AuthConfig;
import com.cognite.client.servicesV1.executor.FileBinaryRequestExecutor;
import com.cognite.client.servicesV1.parser.ItemParser;

import com.cognite.client.dto.*;
import com.cognite.client.servicesV1.executor.RequestExecutor;
import com.cognite.client.servicesV1.parser.FileParser;
import com.cognite.client.servicesV1.parser.LoginStatusParser;
import com.cognite.client.servicesV1.request.*;
import com.cognite.client.servicesV1.response.*;
import com.cognite.client.servicesV1.util.JsonUtil;
import com.cognite.v1.timeseries.proto.DataPointListItem;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import okhttp3.ConnectionSpec;
import okhttp3.HttpUrl;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import okhttp3.OkHttpClient;

import javax.annotation.Nullable;

import static com.cognite.client.servicesV1.ConnectorConstants.*;

/**
 * The service handles connections to the Cognite REST api.
 */
@AutoValue
public abstract class ConnectorServiceV1 implements Serializable {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
    // Logger identifier per instance
    private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
    private final String loggingPrefix = "ConnectorService [" + randomIdString + "] -";

    private static Builder builder() {
        return new com.cognite.client.servicesV1.AutoValue_ConnectorServiceV1.Builder();
    }

    public static ConnectorServiceV1 of(CogniteClient client) {
        return ConnectorServiceV1.builder()
                .setClient(client)
                .build();
    }

    public static ConnectorServiceV1 create() {
        return ConnectorServiceV1.builder().build();
    }

    public abstract CogniteClient getClient();

    /**
     * Read assets from Cognite.
     *
     * @param queryParameters The parameters for the assets query.
     * @return
     */
    public ResultFutureIterator<String> readAssets(Request queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read assets service.");

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("assets/list")
                .setRequest(queryParameters)
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Read assets aggregates from Cognite.
     *
     * @return
     */
    public ItemReader<String> readAssetsAggregates() {
        LOG.debug(loggingPrefix + "Initiating read assets aggregates service.");
        

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("assets/aggregate")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonResponseParser.create());
    }

    /**
     * Read assets by id from Cognite.
     *
     * @return
     */
    public ItemReader<String> readAssetsById() {
        LOG.debug(loggingPrefix + "Initiating read assets by id service.");
        

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("assets/byids")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Write Assets to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeAssets() {
        LOG.debug(loggingPrefix + "Initiating write assets service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("assets")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Update Assets in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter updateAssets() {
        LOG.debug(loggingPrefix + "Initiating update assets service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("assets/update")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Delete Assets in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteAssets() {
        LOG.debug(loggingPrefix + "Initiating delete assets service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("assets/delete")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Read events from Cognite.
     *
     * @param queryParameters The parameters for the events query.
     * @return
     */
    public ResultFutureIterator<String> readEvents(Request queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read events service.");

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("events/list")
                .setRequest(queryParameters)
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Read events aggregates from Cognite.
     *
     * @return
     */
    public ItemReader<String> readEventsAggregates() {
        LOG.debug(loggingPrefix + "Initiating read events aggregates service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("events/aggregate")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonResponseParser.create());
    }

    /**
     * Read events by id from Cognite.
     *
     * @return
     */
    public ItemReader<String> readEventsById() {
        LOG.debug(loggingPrefix + "Initiating read events service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("events/byids")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Write Events to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeEvents() {
        LOG.debug(loggingPrefix + "Initiating write events service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("events")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Update Events in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter updateEvents() {
        LOG.debug(loggingPrefix + "Initiating update events service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("events/update")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Delete Events in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteEvents() {
        LOG.debug(loggingPrefix + "Initiating delete events service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("events/delete")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Fetch sequences headers from Cognite.
     *
     * @param queryParameters The parameters for the events query.
     * @return
     */
    public ResultFutureIterator<String> readSequencesHeaders(Request queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read sequences headers service.");

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("sequences/list")
                .setRequest(queryParameters)
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Read sequences aggregates from Cognite.
     *
     * @return
     */
    public ItemReader<String> readSequencesAggregates() {
        LOG.debug(loggingPrefix + "Initiating read sequences aggregates service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("sequences/aggregate")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonResponseParser.create());
    }

    /**
     * Read sequences by id from Cognite.
     *
     * @return
     */
    public ItemReader<String> readSequencesById() {
        LOG.debug(loggingPrefix + "Initiating read sequences by id service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("sequences/byids")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Write sequences headers to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeSequencesHeaders() {
        LOG.debug(loggingPrefix + "Initiating write sequences headers service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("sequences")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider)
                .withDuplicatesResponseParser(JsonErrorMessageDuplicateResponseParser.builder().build());
    }

    /**
     * Update sequences headers in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter updateSequencesHeaders() {
        LOG.debug(loggingPrefix + "Initiating update sequences headers service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("sequences/update")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Delete sequences headers in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteSequencesHeaders() {
        LOG.debug(loggingPrefix + "Initiating delete sequences service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("sequences/delete")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Fetch sequences rows / body from Cognite.
     *
     * @param queryParameters The parameters for the events query.
     * @return
     */
    public ResultFutureIterator<String> readSequencesRows(Request queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read sequences rows service.");

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("sequences/data/list")
                .setRequest(queryParameters)
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(getClient(), requestProvider, JsonResponseParser.create());
    }

    /**
     * Write sequences rows to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeSequencesRows() {
        LOG.debug(loggingPrefix + "Initiating write sequences rows service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("sequences/data")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Delete sequences rows in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteSequencesRows() {
        LOG.debug(loggingPrefix + "Initiating delete sequences service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("sequences/data/delete")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * List timeseries headers from Cognite.
     *
     * @param queryParameters The parameters for the TS query.
     * @return
     */
    public ResultFutureIterator<String> readTsHeaders(Request queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read TS headers service.");

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("timeseries/list")
                .setRequest(queryParameters)
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Read timeseries aggregates from Cognite.
     *
     * @return
     */
    public ItemReader<String> readTsAggregates() {
        LOG.debug(loggingPrefix + "Initiating read timeseries aggregates service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries/aggregate")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonResponseParser.create());
    }

    /**
     * Read time series headers by id from Cognite.
     *
     * @return
     */
    public ItemReader<String> readTsById() {
        LOG.debug(loggingPrefix + "Initiating read time series by id service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries/byids")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Write time series headers to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeTsHeaders() {
        LOG.debug(loggingPrefix + "Initiating write ts headers service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Update time series headers in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter updateTsHeaders() {
        LOG.debug(loggingPrefix + "Initiating update ts headers service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries/update")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Delete TS headers in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteTsHeaders() {
        LOG.debug(loggingPrefix + "Initiating delete ts service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries/delete")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Fetch timeseries datapoints from Cognite.
     *
     * @param queryParameters The parameters for the events query.
     * @return
     */
    public ResultFutureIterator<String> readTsDatapoints(Request queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read TS datapoints service.");

        TSPointsRequestProvider requestProvider = TSPointsRequestProvider.builder()
                .setEndpoint("timeseries/data/list")
                .setRequest(queryParameters)
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        TSPointsResponseParser responseParser = TSPointsResponseParser.builder().build()
                .withRequest(queryParameters);

        return ResultFutureIterator.<String>of(getClient(), requestProvider, responseParser);
    }

    /**
     * Fetch timeseries datapoints from Cognite using protobuf encoding.
     *
     * @param queryParameters The parameters for the events query.
     * @return
     */
    public ResultFutureIterator<DataPointListItem>
            readTsDatapointsProto(Request queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read TS datapoints service.");

        TSPointsReadProtoRequestProvider requestProvider = TSPointsReadProtoRequestProvider.builder()
                .setEndpoint("timeseries/data/list")
                .setRequest(queryParameters)
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        TSPointsProtoResponseParser responseParser = TSPointsProtoResponseParser.builder().build()
                .withRequest(queryParameters);

        return ResultFutureIterator.<DataPointListItem>of(getClient(), requestProvider, responseParser);
    }

    /**
     * Read latest data point from Cognite.
     *
     * @return
     */
    public ItemReader<String> readTsDatapointsLatest() {
        LOG.debug(loggingPrefix + "Initiating read latest data point service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries/data/latest")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Write time series headers to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeTsDatapoints() {
        LOG.debug(loggingPrefix + "Building writer for ts datapoints service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries/data")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Write time series data points to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeTsDatapointsProto() {
        LOG.debug(loggingPrefix + "Building writer for ts datapoints service.");

        TSPointsWriteProtoRequestProvider requestProvider = TSPointsWriteProtoRequestProvider.builder()
                .setEndpoint("timeseries/data")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Delete data points in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteDatapoints() {
        LOG.debug(loggingPrefix + "Initiating delete data points service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("timeseries/data/delete")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Fetch 3d models from Cognite.
     *
     * @param queryParameters The parameters for the events query.
     * @return
     */
    public Iterator<CompletableFuture<ResponseItems<String>>> read3dModels(Request queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read 3d models service.");

        GetSimpleListRequestProvider requestProvider = GetSimpleListRequestProvider.builder()
                .setEndpoint("3d/models")
                .setRequest(queryParameters)
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Fetch Raw rows from Cognite. This service can handle both single rows and
     * large collection of rows.
     *
     * @param queryParameters The parameters for the raw query.
     * @return
     */
    public ResultFutureIterator<String> readRawRows(Request queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read raw rows service.");

        RawReadRowsRequestProvider requestProvider = RawReadRowsRequestProvider.builder()
                .setEndpoint("raw/dbs")
                .setRequest(queryParameters)
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(getClient(), requestProvider, JsonRawRowResponseParser.builder().build());
    }

    /**
     * Fetch a single row by row key.
     *
     * @return
     */
    public ItemReader<String> readRawRow() {
        LOG.debug(loggingPrefix + "Initiating read single row service.");

        RawReadRowsRequestProvider requestProvider = RawReadRowsRequestProvider.builder()
                .setEndpoint("raw/dbs")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.<String>of(getClient(),
                requestProvider, JsonRawRowResponseParser.builder().build());
    }

    /**
     * Read cursors for retrieving events in parallel. The results set is split into n partitions.
     *
     * @return
     */
    public ItemReader<String> readCursorsRawRows() {
        LOG.debug(loggingPrefix + "Initiating read raw cursors service.");

        RawReadRowsCursorsRequestProvider requestProvider = RawReadRowsCursorsRequestProvider.builder()
                .setEndpoint("raw/dbs")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Write rows to Raw in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     */
    public ItemWriter writeRawRows() {
        LOG.debug(loggingPrefix + "Initiating write raw rows service.");

        RawWriteRowsRequestProvider requestProvider = RawWriteRowsRequestProvider.builder()
                .setEndpoint("raw/dbs")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Delete Assets in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteRawRows() {
        LOG.debug(loggingPrefix + "Initiating delete raw rows service.");

        RawDeleteRowsRequestProvider requestProvider = RawDeleteRowsRequestProvider.builder()
                .setEndpoint("raw/dbs")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * List the Raw database names from Cognite.
     *
     * @return
     */
    public ResultFutureIterator<String> readRawDbNames(AuthConfig config) {
        LOG.debug(loggingPrefix + "Initiating read raw database names service.");

        GetSimpleListRequestProvider requestProvider = GetSimpleListRequestProvider.builder()
                .setEndpoint("raw/dbs")
                .setRequest(Request.create()
                        .withRootParameter("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE)
                        .withAuthConfig(config))
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Create Raw tables in a given database.
     *
     * @return
     */
    public ItemWriter writeRawDbNames() {
        LOG.debug(loggingPrefix + "Creating databases");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("raw/dbs")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Create Raw tables in a given database.
     *
     * @return
     */
    public ItemWriter deleteRawDbNames() {
        LOG.debug(loggingPrefix + "Deleting databases");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("raw/dbs/delete")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * List the Raw tables for a given database.
     *
     * @param dbName The name of the database to list tables from.
     * @return
     */
    public ResultFutureIterator<String> readRawTableNames(String dbName, AuthConfig config) {
        Preconditions.checkNotNull(dbName);
        Preconditions.checkArgument(!dbName.isEmpty(), "Database name cannot be empty.");
        LOG.debug(loggingPrefix + "Listing tables for database {}", dbName);

        GetSimpleListRequestProvider requestProvider = GetSimpleListRequestProvider.builder()
                .setEndpoint("raw/dbs/" + dbName + "/tables")
                .setRequest(Request.create()
                        .withRootParameter("limit", ConnectorConstants.DEFAULT_MAX_BATCH_SIZE)
                        .withAuthConfig(config))
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Create Raw tables in a given database.
     *
     * @param dbName The name of the database to list tables from.
     * @return
     */
    public ItemWriter writeRawTableNames(String dbName) {
        Preconditions.checkNotNull(dbName);
        Preconditions.checkArgument(!dbName.isEmpty(), "Database name cannot be empty.");
        LOG.debug(loggingPrefix + "Creating tables in database {}", dbName);

        RawWriteTablesRequestProvider requestProvider = RawWriteTablesRequestProvider.builder()
                .setEndpoint("raw/dbs/" + dbName + "/tables")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Delete Raw tables from a given database.
     *
     * @param dbName The name of the database to list tables from.
     * @return
     */
    public ItemWriter deleteRawTableNames(String dbName) {
        Preconditions.checkNotNull(dbName);
        Preconditions.checkArgument(!dbName.isEmpty(), "Database name cannot be empty.");
        LOG.debug(loggingPrefix + "Deleting tables in database {}", dbName);

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("raw/dbs/" + dbName + "/tables/delete")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * List file headers from Cognite.
     *
     * @param queryParameters The parameters for the file query.
     * @return
     */
    public ResultFutureIterator<String> readFileHeaders(Request queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read File headers service.");

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("files/list")
                .setRequest(queryParameters)
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Read files aggregates from Cognite.
     *
     * @return
     */
    public ItemReader<String> readFilesAggregates() {
        LOG.debug(loggingPrefix + "Initiating read files aggregates service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("files/aggregate")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonResponseParser.create());
    }

    /**
     * Read files by id from Cognite.
     *
     * @return
     */
    public ItemReader<String> readFilesById() {
        LOG.debug(loggingPrefix + "Initiating read files by id service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("files/byids")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Read file binaries from Cognite.
     *
     * Returns an <code>FileBinaryReader</code> which can be used to read file binaries by id.
     *
     * @return
     */
    public FileBinaryReader readFileBinariesByIds() {
        LOG.debug(loggingPrefix + "Initiating read File binaries by ids service.");

        return FileBinaryReader.of(getClient());
    }

    /**
     * Write files to Cognite. This is a two-step request with 1) post the file metadata / header
     * and 2) post the file binary.
     *
     * This method returns an <code>ItemWriter</code> to which you can post the file metadata / header.
     * I.e. it performs step 1), but not step 2). The response from the <code>ItemWriter</code>
     * contains a URL reference for the file binary upload.
     *
     * @return
     */
    public ItemWriter writeFileHeaders() {
        LOG.debug(loggingPrefix + "Initiating write file header / metadata service.");

        FilesUploadRequestProvider requestProvider = FilesUploadRequestProvider.builder()
                .setEndpoint("files")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Write files to Cognite.
     *
     * This method returns an <code>FileWriter</code> to which you can post the <code>FileContainer</code>
     * with file metadata / header. I.e. this writer allows you to post both the file header and the
     * file binary in a single method call. The response contains the the metadata response item for the file.
     *
     * @return
     */
    public FileWriter writeFileProto() {
        LOG.debug(loggingPrefix + "Initiating write file proto service.");

        return FileWriter.of(getClient());
    }

    /**
     * Update file headers to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter updateFileHeaders() {
        LOG.debug(loggingPrefix + "Initiating update file headers service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("files/update")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Delete Files (including headers) in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteFiles() {
        LOG.debug(loggingPrefix + "Initiating delete files service.");
        

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("files/delete")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Get the login status from Cognite.
     *
     * @param host
     * @return
     */
    public LoginStatus readLoginStatusByApiKey(String host) throws Exception {
        LOG.debug(loggingPrefix + "Getting login status for host [{}].", host);

        GetLoginRequestProvider requestProvider = GetLoginRequestProvider.builder()
                .setEndpoint("status")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        JsonResponseParser responseParser = JsonResponseParser.create();

        SingleRequestItemReader<String> itemReader =
                SingleRequestItemReader.of(getClient(), requestProvider, responseParser);

        // Send the request to the Cognite api
        ResponseItems<String> responseItems = itemReader.getItems(Request.create()
                .withAuthConfig(AuthConfig.create()
                        .withHost(host)));

        // Check the response
        if (!responseItems.getResponseBinary().getResponse().isSuccessful()
                || responseItems.getResultsItems().size() != 1) {
            String message = loggingPrefix + "Cannot get login status from Cognite. Could not get a valid response.";
            LOG.error(message);
            throw new Exception(message);
        }

        // parser the response
        return LoginStatusParser.parseLoginStatus(responseItems.getResultsItems().get(0));
    }

    /**
     * Fetch relationships from Cognite.
     *
     * @param queryParameters The parameters for the data sets query.
     * @return
     */
    public ResultFutureIterator<String> readRelationships(Request queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read relationships service.");

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("relationships/list")
                .setRequest(queryParameters)
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .setBetaEnabled(true)
                .build();

        return ResultFutureIterator.<String>of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Read relationships by id from Cognite.
     *
     * @return
     */
    public ItemReader<String> readRelationshipsById() {
        LOG.debug(loggingPrefix + "Initiating read relationships by id service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("relationships/byids")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Write relationships to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeRelationships() {
        LOG.debug(loggingPrefix + "Initiating write relationships service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("relationships")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Update relationships to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter updateRelationships() {
        LOG.debug(loggingPrefix + "Initiating update relationships service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("relationships/update")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Delete relationships in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteRelationships() {
        LOG.debug(loggingPrefix + "Initiating delete relationships service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("relationships/delete")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Update data sets in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter updateDataSets() {
        LOG.debug(loggingPrefix + "Initiating update data sets service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("datasets/update")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Fetch data sets from Cognite.
     *
     * @param queryParameters The parameters for the data sets query.
     * @return
     */
    public ResultFutureIterator<String> readDataSets(Request queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read data sets service.");

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("datasets/list")
                .setRequest(queryParameters)
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Read data sets aggregates from Cognite.
     *
     * @return
     */
    public ItemReader<String> readDataSetsAggregates() {
        LOG.debug(loggingPrefix + "Initiating read data set aggregates service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("datasets/aggregate")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonResponseParser.create());
    }

    /**
     * Read data sets by id from Cognite.
     *
     * @return
     */
    public ItemReader<String> readDataSetsById() {
        LOG.debug(loggingPrefix + "Initiating read data sets by id service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("datasets/byids")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Write data sets to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeDataSets() {
        LOG.debug(loggingPrefix + "Initiating write data sets service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("datasets")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Read labels from Cognite.
     *
     * @param queryParameters The parameters for the data sets query.
     * @return
     */
    public ResultFutureIterator<String> readLabels(Request queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read labels service.");

        PostJsonListRequestProvider requestProvider = PostJsonListRequestProvider.builder()
                .setEndpoint("labels/list")
                .setRequest(queryParameters)
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Write labels to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeLabels() {
        LOG.debug(loggingPrefix + "Initiating write labels service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("labels")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Delete labels in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteLabels() {
        LOG.debug(loggingPrefix + "Initiating delete labels service.");

        PostPlaygroundJsonRequestProvider requestProvider = PostPlaygroundJsonRequestProvider.builder()
                .setEndpoint("labels/delete")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Read security categories from Cognite.
     *
     * @param queryParameters The parameters for the security categories query.
     * @return
     */
    public ResultFutureIterator<String> readSecurityCategories(Request queryParameters) {
        LOG.debug(loggingPrefix + "Initiating read security categories service.");

        GetSimpleListRequestProvider requestProvider = GetSimpleListRequestProvider.builder()
                .setEndpoint("securitycategories")
                .setRequest(queryParameters)
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ResultFutureIterator.<String>of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Write security categories to Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter writeSecurityCategories() {
        LOG.debug(loggingPrefix + "Initiating write security categories service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("securitycategories")
                .setRequest(Request.create())
                .setSdkIdentifier(SDK_IDENTIFIER)
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Delete security categories in Cognite.
     *
     * Calling this method will return an <code>ItemWriter</code>
     * @return
     */
    public ItemWriter deleteSecurityCategories() {
        LOG.debug(loggingPrefix + "Initiating delete security categories service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("securitycategories/delete")
                .setRequest(Request.create())
                .setSdkIdentifier(SDK_IDENTIFIER)
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Detect references to assets and files in an engineering diagram and annotate the references with bounding boxes.
     * Finds entities in the engineering diagram that match a list of entity names,
     * for instance asset names. The P&ID must be a single-page PDF file.
     *
     * @return
     */
    public ItemReader<String> detectAnnotationsDiagrams() {
        PostPlaygroundJsonRequestProvider jobStartRequestProvider =
                PostPlaygroundJsonRequestProvider.builder()
                        .setEndpoint("context/diagram/detect")
                        .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                        .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                        .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                        .build();

        RequestParametersResponseParser jobStartResponseParser = RequestParametersResponseParser.of(
                ImmutableMap.of("jobId", "jobId"));

        GetPlaygroundJobIdRequestProvider jobResultsRequestProvider = GetPlaygroundJobIdRequestProvider.of("context/diagram/detect")
                .toBuilder()
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return AsyncJobReader.of(getClient(), jobStartRequestProvider, jobResultsRequestProvider, JsonItemResponseParser.create())
                .withJobStartResponseParser(jobStartResponseParser);
    }

    /**
     * Convert an engineering diagram in PDF format to an interactive SVG where
     * the provided annotations are highlighted.
     *
     * @return
     */
    public ItemReader<String> convertDiagrams() {
        PostPlaygroundJsonRequestProvider jobStartRequestProvider =
                PostPlaygroundJsonRequestProvider.builder()
                        .setEndpoint("context/diagram/convert")
                        .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                        .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                        .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                        .build();

        RequestParametersResponseParser jobStartResponseParser = RequestParametersResponseParser.of(
                ImmutableMap.of("jobId", "jobId"));

        GetPlaygroundJobIdRequestProvider jobResultsRequestProvider = GetPlaygroundJobIdRequestProvider.of("context/diagram/convert")
                .toBuilder()
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return AsyncJobReader.of(getClient(), jobStartRequestProvider, jobResultsRequestProvider, JsonItemResponseParser.create())
                .withJobStartResponseParser(jobStartResponseParser);
    }

    /**
     * Create a reader for listing entity matcher models.
     *
     * @return An {@link ItemReader<String>} for reading the models.
     */
    public ItemReader<String> readEntityMatcherModels() {
        LOG.debug(loggingPrefix + "Initiating read entity matcher models service.");

        // todo Implement new list entity matcher models
        GetPlaygroundRequestProvider requestProvider = GetPlaygroundRequestProvider.builder()
                .setEndpoint("context/entity_matching")
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return SingleRequestItemReader.of(getClient(), requestProvider, JsonItemResponseParser.create());
    }

    /**
     * Create a writer for deleting entity matcher models.
     *
     * @return An {@link ItemWriter} for deleting the models
     */
    public ItemWriter deleteEntityMatcherModels() {
        LOG.debug(loggingPrefix + "Initiating delete entity matcher models service.");

        PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                .setEndpoint("context/entitymatching/delete")
                .setRequest(Request.create())
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return ItemWriter.of(getClient(), requestProvider);
    }

    /**
     * Create an entity matcher predict reader.
     *
     * @return
     */
    public ItemReader<String> entityMatcherPredict() {
        LOG.debug(loggingPrefix + "Initiating entity matcher predict service.");

        PostJsonRequestProvider jobStartRequestProvider =
                PostJsonRequestProvider.builder()
                        .setEndpoint("context/entitymatching/predict")
                        .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                        .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                        .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                        .build();

        RequestParametersResponseParser jobStartResponseParser = RequestParametersResponseParser
                .of(ImmutableMap.of("jobId", "id"));

        GetIdRequestProvider jobResultsRequestProvider =
                GetIdRequestProvider.of("context/entitymatching/jobs")
                .toBuilder()
                .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                .build();

        return AsyncJobReader.of(getClient(), jobStartRequestProvider, jobResultsRequestProvider, JsonItemResponseParser.create())
                .withJobStartResponseParser(jobStartResponseParser);
    }

    /**
     * Create an entity matcher training executor.
     *
     * @return
     */
    public Connector<String> entityMatcherFit() {
        LOG.debug(loggingPrefix + "Initiating entity matcher training service.");

        PostJsonRequestProvider jobStartRequestProvider =
                PostJsonRequestProvider.builder()
                        .setEndpoint("context/entitymatching")
                        .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                        .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                        .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                        .build();

        RequestParametersResponseParser jobStartResponseParser = RequestParametersResponseParser.of(
                ImmutableMap.of("id", "id"));

        GetIdRequestProvider jobResultsRequestProvider =
                GetIdRequestProvider.of("context/entitymatching")
                        .toBuilder()
                        .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                        .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                        .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                        .build();

        return AsyncJobReader.of(getClient(), jobStartRequestProvider, jobResultsRequestProvider, JsonResponseParser.create())
                .withJobStartResponseParser(jobStartResponseParser);
    }

    // TODO 3d models, revisions and nodes


    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setClient(CogniteClient value);
        abstract ConnectorServiceV1 build();
    }

    /**
     * Base class for read and write requests.
     */
    abstract static class ConnectorBase implements Serializable {
        // Default response parsers
        static final ResponseParser<String> DEFAULT_RESPONSE_PARSER = JsonItemResponseParser.create();
        static final ResponseParser<String> DEFAULT_DUPLICATES_RESPONSE_PARSER = JsonErrorItemResponseParser.builder()
                .setErrorSubPath("duplicated")
                .build();
        static final ResponseParser<String> DEFAULT_MISSING_RESPONSE_PARSER = JsonErrorItemResponseParser.builder()
                .setErrorSubPath("missing")
                .build();

        final Logger LOG = LoggerFactory.getLogger(this.getClass());

        abstract RequestProvider getRequestProvider();
        abstract CogniteClient getClient();
        abstract RequestExecutor getRequestExecutor();

        abstract static class Builder<B extends Builder<B>> {
            abstract B setRequestProvider(RequestProvider value);
            abstract B setClient(CogniteClient value);
            abstract B setRequestExecutor(RequestExecutor value);
        }
    }

    /**
     * Iterator for paging through requests based on response cursors.
     *
     * This iterator is based on async request, and will return a {@link CompletableFuture} on each
     * {@code next()} call.
     *
     * However, {@code hasNext()} needs to wait for the current request to complete before being
     * able to evaluate if it carries a cursor to the next page.
     *
     * @param <T>
     */
    @AutoValue
    public abstract static class ResultFutureIterator<T>
            extends ConnectorBase implements Iterator<CompletableFuture<ResponseItems<T>>> {
        private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
        private final String loggingPrefix = "ResultFutureIterator [" + randomIdString + "] -";

        private CompletableFuture<ResponseItems<T>> currentResponseFuture = null;

        private static <T> Builder<T> builder() {
            return new com.cognite.client.servicesV1.AutoValue_ConnectorServiceV1_ResultFutureIterator.Builder<T>();
        }

        public static <T> ResultFutureIterator<T> of(CogniteClient client,
                                                     RequestProvider requestProvider,
                                                     ResponseParser<T> responseParser) {
            return ResultFutureIterator.<T>builder()
                    .setClient(client)
                    .setRequestExecutor(RequestExecutor.of(client.getHttpClient())
                            .withExecutor(client.getExecutorService())
                            .withMaxRetries(client.getClientConfig().getMaxRetries()))
                    .setRequestProvider(requestProvider)
                    .setResponseParser(responseParser)
                    .build();
        }

        abstract ResponseParser<T> getResponseParser();

        @Override
        public boolean hasNext() {
            // must wrap the logic in a try-catch since <hasNext> does not allow us to just re-throw the exceptions.
            try {
                if (currentResponseFuture == null) {
                    // Have not issued any request yet, so always return true to perform the first request.
                    return true;
                } else {
                    // We are past the first request. Check the current item if it has a next cursor
                    return getResponseParser().extractNextCursor(
                            currentResponseFuture.join().getResponseBinary().getResponseBodyBytes().toByteArray()
                            ).isPresent();
                }
            } catch (Exception e) {
                LOG.error(loggingPrefix + "Error when executing check for <hasNext>.", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public CompletableFuture<ResponseItems<T>> next() throws NoSuchElementException {
            if (!this.hasNext()) {
                LOG.warn(loggingPrefix + "Client calling next() when no more elements are left to iterate over");
                throw new NoSuchElementException("No more elements to iterate over.");
            }
            try {
                Optional<String> nextCursor = Optional.empty();
                if (null != currentResponseFuture) {
                    nextCursor = getResponseParser().extractNextCursor(
                            currentResponseFuture.join().getResponseBinary().getResponseBodyBytes().toByteArray()
                            );
                    LOG.debug(loggingPrefix + "More items to iterate over. Building next api request based on cursor: {}",
                            nextCursor.orElse("could not identify cursor--should be investigated"));

                    // should not happen, but let's check just in case.
                    if (!nextCursor.isPresent()) {
                        String message = loggingPrefix + "Invalid state. <hasNext()> indicated one more element,"
                                + " but no next cursor is found.";
                        LOG.error(message);
                        throw new Exception(message);
                    }
                } else {
                    LOG.debug(loggingPrefix + "Building first api request of the iterator.");
                }
                okhttp3.Request request = this.getRequestProvider().buildRequest(nextCursor);
                LOG.debug(loggingPrefix + "Built request for URL: {}", request.url().toString());

                // Execute the request and get the response future
                CompletableFuture<ResponseItems<T>> responseItemsFuture = getRequestExecutor()
                        .executeRequestAsync(request)
                        .thenApply(responseBinary ->
                            ResponseItems.of(getResponseParser(), responseBinary));

                currentResponseFuture = responseItemsFuture;
                return responseItemsFuture;

            } catch (Exception e) {
                String message = loggingPrefix + "Failed to get more elements when requesting new batch from Fusion: ";
                LOG.error(message, e);
                throw new NoSuchElementException(message + System.lineSeparator() + e.getMessage());
            }
        }

        @AutoValue.Builder
        abstract static class Builder<T> extends ConnectorBase.Builder<Builder<T>> {
            abstract Builder<T> setResponseParser(ResponseParser<T> value);

            abstract ResultFutureIterator<T> build();
        }
    }

    /**
     * Reads items from the Cognite API.
     *
     * This reader targets API endpoints which complete its operation in a single request.
     */
    @AutoValue
    public static abstract class SingleRequestItemReader<T> extends ConnectorBase implements ItemReader<T> {
        private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
        private final String loggingPrefix = "SingleRequestItemReader [" + randomIdString + "] -";

        static <T> Builder<T> builder() {
            return new com.cognite.client.servicesV1.AutoValue_ConnectorServiceV1_SingleRequestItemReader.Builder<T>();
        }

        /**
         * Creates an instance of this reader. You must provide a {@link RequestProvider} and a
         * {@link ResponseParser}.
         *
         * @param requestProvider
         * @param responseParser
         * @param <T>
         * @return
         */
        static <T> SingleRequestItemReader<T> of(CogniteClient client,
                                                 RequestProvider requestProvider,
                                                 ResponseParser<T> responseParser) {
            return SingleRequestItemReader.<T>builder()
                    .setClient(client)
                    .setRequestExecutor(RequestExecutor.of(client.getHttpClient())
                            .withExecutor(client.getExecutorService())
                            .withMaxRetries(client.getClientConfig().getMaxRetries())
                            .withValidResponseCodes(ImmutableList.of(400, 401, 409, 422)))
                    .setRequestProvider(requestProvider)
                    .setResponseParser(responseParser)
                    .build();
        }

        abstract ResponseParser<T> getResponseParser();

        /**
         * Executes a request to get items and blocks the thread until all items have been downloaded.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public ResponseItems<T> getItems(Request items) throws Exception {
            return this.getItemsAsync(items).join();
        }

        /**
         * Executes an item-based request to get items asynchronously.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public CompletableFuture<ResponseItems<T>> getItemsAsync(Request items) throws Exception {
            Preconditions.checkNotNull(items, "Input cannot be null.");

            // Execute the request and get the response future
            CompletableFuture<ResponseItems<T>> responseItemsFuture = getRequestExecutor()
                    .executeRequestAsync(getRequestProvider()
                            .withRequest(items)
                            .buildRequest(Optional.empty()))
                    .thenApply(responseBinary ->
                        ResponseItems.of(getResponseParser(), responseBinary));

            return responseItemsFuture;
        }

        @AutoValue.Builder
        abstract static class Builder<T> extends ConnectorBase.Builder<Builder<T>> {
            abstract Builder<T> setResponseParser(ResponseParser<T> value);

            abstract SingleRequestItemReader<T> build();
        }
    }

    /**
     * Reads items from the Cognite API.
     *
     * This reader targets API endpoints which complete its operation over two requests. I.e. async api
     * pattern where the first request starts the job and the second request collects the result.
     *
     * The reader works in two steps:
     * 1. Issue a "start job" request.
     * 2. Start a polling loop issuing "get job results/status" requests.
     *
     * The "start job" step is executed using the input {@link Request}, the configured
     * {@code RequestProvider} "jobStartRequestProvider" and the configured {@code JobStartResponseParser}.
     * This request will typically yield a response which contains the async job identification
     * parameters ({@code jobId}, {@code modelId}, etc.).
     * These identification parameters are again used as input for step 2, "get job results".
     *
     * The output from step 1 is a {@link Request} object representing the parameters to be used
     * as input to step 2, "get job results/update". The {@link Request} is interpreted by the
     * {@code JobResultRequestProvider} and the api will regularly be polled for job results. When the api provides
     * job results, this response is routed to the {@code ResponseParser} and returned.
     *
     */
    @AutoValue
    public static abstract class AsyncJobReader<T> extends ConnectorBase implements Connector<T>, ItemReader<T> {
        private static final int DEFAULT_MAX_WORKERS = 32;
        private static final ForkJoinPool DEFAULT_POOL = new ForkJoinPool();

        // parsers for various job status attributes
        private static final JsonStringAttributeResponseParser jobStatusResponseParser =
                JsonStringAttributeResponseParser.create().withAttributePath("status");
        private static final JsonStringAttributeResponseParser errorMessageResponseParser =
                JsonStringAttributeResponseParser.create().withAttributePath("errorMessage");

        // default request and response parser / providers
        private static final RequestParametersResponseParser DEFAULT_JOB_START_RESPONSE_PARSER =
                RequestParametersResponseParser.of(ImmutableMap.of(
                        "modelId", "modelId",
                        "jobId", "jobId",
                        "id", "id"
                ));

        private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
        private final String loggingPrefix = "AsyncJobReader [" + randomIdString + "] -";
        private final ObjectMapper objectMapper = JsonUtil.getObjectMapperInstance();

        static <T> Builder<T> builder() {
            return new com.cognite.client.servicesV1.AutoValue_ConnectorServiceV1_AsyncJobReader.Builder<T>()
                    .setJobStartResponseParser(DEFAULT_JOB_START_RESPONSE_PARSER)
                    .setJobTimeoutDuration(DEFAULT_ASYNC_API_JOB_TIMEOUT)
                    .setPollingInterval(DEFAULT_ASYNC_API_JOB_POLLING_INTERVAL)
                    ;
        }

        /**
         * Creates an instance of this reader. You must provide a {@link RequestProvider} and a
         * {@link ResponseParser}.
         *
         * @param jobStartRequestProvider
         * @param jobResultRequestProvider
         * @param responseParser
         * @param <T>
         * @return
         */
        static <T> AsyncJobReader<T> of(CogniteClient client,
                                        RequestProvider jobStartRequestProvider,
                                        RequestProvider jobResultRequestProvider,
                                        ResponseParser<T> responseParser) {
            return AsyncJobReader.<T>builder()
                    .setClient(client)
                    .setRequestExecutor(RequestExecutor.of(client.getHttpClient())
                            .withExecutor(client.getExecutorService())
                            .withMaxRetries(client.getClientConfig().getMaxRetries())
                            .withValidResponseCodes(ImmutableList.of(400, 401, 409, 422)))
                    .setRequestProvider(jobStartRequestProvider)
                    .setJobResultRequestProvider(jobResultRequestProvider)
                    .setResponseParser(responseParser)
                    .build();
        }

        abstract Builder<T> toBuilder();
        abstract ResponseParser<T> getResponseParser();
        abstract ResponseParser<Request> getJobStartResponseParser();
        abstract RequestProvider getJobResultRequestProvider();
        abstract Duration getJobTimeoutDuration();
        abstract Duration getPollingInterval();

        /**
         * Sets the {@link ResponseParser} for the first/initial api request. This request will typically be
         * the job start/register request. This {@link ResponseParser} is responsible for generating the
         * {@link Request} for the second api request--the request that retrieves the job results.
         *
         * The job results request is built by the {@code JobResultRequestProvider}.
         *
         * @param responseParser
         * @return
         */
        AsyncJobReader<T> withJobStartResponseParser(ResponseParser<Request> responseParser) {
            return toBuilder().setJobStartResponseParser(responseParser).build();
        }

        /**
         * Set the {@code RequestProvider} for the second api request. This request will typically
         * retrieve the job results. This request provider receives its {@link Request} from
         * the {@code JobStartResponseParser}.
         *
         * @param requestProvider
         * @return
         */
        AsyncJobReader<T> withJobResultRequestProvider(RequestProvider requestProvider) {
            return toBuilder().setJobResultRequestProvider(requestProvider).build();
        }

        /**
         * Sets the timeout for the api job. This reader will wait for up to the timeout duration for the api
         * to complete the job.
         *
         * When the timeout is triggered, the reader will respond with the current job status (most likely
         * "Queued" or "Running").
         *
         * The default timeout is 15 minutes.
         *
         * @param timeout
         * @return
         */
        AsyncJobReader<T> withJobTimeout(Duration timeout) {
            return toBuilder().setJobTimeoutDuration(timeout).build();
        }

        /**
         * Sets the polling interval. The polling interval determines how often the reader requests a status
         * update from the api. This will affect the perceived responsiveness of the reader.
         *
         * Don't set the polling interval too low, or you risk overloading the api.
         *
         * The default polling interval is 2 sec.
         *
         * @param interval
         * @return
         */
        AsyncJobReader<T> withPollingInterval(Duration interval) {
            return toBuilder().setPollingInterval(interval).build();
        }

        /**
         * Executes a request to get items and blocks the thread until all items have been downloaded.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public ResponseItems<T> execute(Request items) throws Exception {
            return this.getItemsAsync(items).join();
        }

        /**
         * Executes an item-based request to get items asynchronously.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public CompletableFuture<ResponseItems<T>> executeAsync(Request items) throws Exception {
            return this.getItemsAsync(items);
        }

        /**
         * Executes a request to get items and blocks the thread until all items have been downloaded.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public ResponseItems<T> getItems(Request items) throws Exception {
            return this.getItemsAsync(items).join();
        }

        /**
         * Executes an item-based request to get items asynchronously.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public CompletableFuture<ResponseItems<T>> getItemsAsync(Request items) throws Exception {
            Preconditions.checkNotNull(items, "Input cannot be null.");
            LOG.debug(loggingPrefix + "Starting async api job.");

            // Execute the request and get the response future
            CompletableFuture<ResponseItems<T>> responseItemsFuture = getRequestExecutor()
                    .executeRequestAsync(getRequestProvider()
                            .withRequest(items)
                            .buildRequest(Optional.empty()))
                    .thenApply(responseBinary ->
                            ResponseItems.of(getJobStartResponseParser(), responseBinary)
                                    .withErrorMessageResponseParser(errorMessageResponseParser)
                                    .withStatusResponseParser(jobStatusResponseParser))
                    .thenApplyAsync(responseItems -> {
                        try {
                            List<String> responseStatus = responseItems.getStatus();
                            List<Request> resultsItems = responseItems.getResultsItems();

                            if (responseItems.isSuccessful()
                                    && !responseStatus.isEmpty()
                                    && !responseStatus.get(0).equalsIgnoreCase("Failed")
                                    && !resultsItems.isEmpty()) {
                                // We successfully started the async job
                                LOG.info(loggingPrefix + "The async api job successfully started with status: {},"
                                        + "Id: {}, JobId: {}",
                                        responseStatus.get(0),
                                        ItemParser.parseString(responseItems.getResponseBodyAsString(), "id")
                                                .orElse("[no id]"),
                                        ItemParser.parseString(responseItems.getResponseBodyAsString(), "jobId")
                                                .orElse("[no JobId]"));

                                // Activate the get results loop and return the end result.
                                // Must copy the project config (auth details) from the original request.
                                return getJobResult(resultsItems.get(0).withAuthConfig(items.getAuthConfig()),
                                        items);
                            } else {
                                // The async job did not start successfully
                                LOG.warn(loggingPrefix + "The async api job failed to start. Status: {}. Response payload: {} "
                                        + " Response headers: {}",
                                        !responseStatus.isEmpty() ? responseStatus.get(0) : "<no status available>",
                                        responseItems.getResponseBodyAsString(),
                                        responseItems.getResponseBinary().getResponse().headers());

                                return ResponseItems.of(getResponseParser(), responseItems.getResponseBinary());
                            }
                        } catch (Exception e) {
                            String message = String.format(loggingPrefix + "The async api job failed to start. Response headers: %s%n"
                                    + "Response summary: %s%n"
                                    + " Exception: %s",
                                    responseItems.getResponseBinary().getResponse().headers().toString(),
                                    responseItems.getResponseBodyAsString(),
                                    e.getMessage());
                            LOG.error(message);
                           throw new CompletionException(new Exception(message));
                        }
                    }, DEFAULT_POOL)
                    ;

            return responseItemsFuture;
        }

        /*
        Get the async job results via a polling loop.
         */
        private ResponseItems<T> getJobResult(Request request, Request jobStartRequest) {
            Preconditions.checkNotNull(request, "Request cannot be null.");
            Preconditions.checkNotNull(request, "jobStartRequest cannot be null.");

            long timeStart = System.currentTimeMillis();
            long timeMax = System.currentTimeMillis() + getJobTimeoutDuration().toMillis();
            Map<String, Object> jobResultItems = null;
            ResponseBinary jobResultResponse = null;
            boolean jobComplete = false;

            LOG.info(loggingPrefix + "Start polling the async api for results.");
            while (!jobComplete && System.currentTimeMillis() < timeMax) {
                try {
                    Thread.sleep(getPollingInterval().toMillis());

                    jobResultResponse = getRequestExecutor()
                            .executeRequest(getJobResultRequestProvider()
                                    .withRequest(request)
                                    .buildRequest(Optional.empty()));

                    String payload = jobResultResponse.getResponseBodyBytes().toStringUtf8();
                    jobResultItems = objectMapper
                            .readValue(payload, new TypeReference<Map<String, Object>>() {});

                    if (jobResultItems.containsKey("errorMessage")
                            && ((String) jobResultItems.get("status")).equalsIgnoreCase("Failed")) {
                        // The api job has failed.
                        String message = String.format(loggingPrefix
                                        + "Error occurred when completing the async api job. "
                                        + "Status: Failed. Error message: %s %n"
                                        + "Request: %s"
                                        + "Response headers: %s"
                                        + "Job start request: %s",
                                jobResultItems.get("errorMessage"),
                                request.getRequestParametersAsJson(),
                                jobResultResponse.getResponse().headers(),
                                jobStartRequest.getRequestParametersAsJson().substring(0,
                                        Math.min(300, jobStartRequest.getRequestParametersAsJson().length())));
                        LOG.error(message);
                        throw new Exception(message);
                    }

                    if (((String) jobResultItems.get("status")).equalsIgnoreCase("Completed")) {
                        jobComplete = true;
                    } else {
                        LOG.debug(loggingPrefix + "Async job not completed. Status : {}",
                                jobResultItems.get("status"));
                    }
                } catch (Exception e) {
                    LOG.error(loggingPrefix + "Failed to get async api job results. Exception: {}. ",
                            e.getMessage());
                    throw new CompletionException(e);
                }
            }

            if (jobComplete) {
                LOG.info(loggingPrefix + "Job results received.");
                // Try to register job queue time
                if (null != jobResultItems
                        && jobResultItems.containsKey("requestTimestamp")
                        && null != jobResultItems.get("requestTimestamp")
                        && jobResultItems.containsKey("startTimestamp")
                        && null != jobResultItems.get("startTimestamp")) {
                    long requestTimestamp = (Long) jobResultItems.get("requestTimestamp");
                    long startTimestamp = (Long) jobResultItems.get("startTimestamp");
                    jobResultResponse = jobResultResponse
                            .withApiJobQueueDuration(startTimestamp - requestTimestamp);
                }
                // Try to register job execution time
                if (null != jobResultItems
                        && jobResultItems.containsKey("startTimestamp")
                        && null != jobResultItems.get("startTimestamp")
                        && jobResultItems.containsKey("statusTimestamp")
                        && null != jobResultItems.get("statusTimestamp")) {
                    long startTimestamp = (Long) jobResultItems.get("startTimestamp");
                    long statusTimestamp = (Long) jobResultItems.get("statusTimestamp");
                    jobResultResponse = jobResultResponse
                            .withApiJobDuration(statusTimestamp - startTimestamp);
                } else {
                    // register job execution time using the local time registrations
                    jobResultResponse = jobResultResponse
                            .withApiJobDuration(System.currentTimeMillis() - timeStart);
                }
            } else {
                String message = "The api job did not complete within the given timeout. Request parameters: "
                        + request.getRequestParameters().toString();
                LOG.error(loggingPrefix + message);
                throw new CompletionException(
                        new Throwable(message + " Job status: " + jobResultItems.get("status")));
            }

            ResponseItems<T> responseItems = ResponseItems.of(getResponseParser(), jobResultResponse)
                    .withStatusResponseParser(jobStatusResponseParser)
                    .withErrorMessageResponseParser(errorMessageResponseParser);

            return responseItems;
        }

        @AutoValue.Builder
        abstract static class Builder<T> extends ConnectorBase.Builder<Builder<T>> {
            abstract Builder<T> setResponseParser(ResponseParser<T> value);
            abstract Builder<T> setJobStartResponseParser(ResponseParser<Request> value);
            abstract Builder<T> setJobResultRequestProvider(RequestProvider value);
            abstract Builder<T> setJobTimeoutDuration(Duration value);
            abstract Builder<T> setPollingInterval(Duration value);

            abstract AsyncJobReader<T> build();
        }
    }

    @AutoValue
    public static abstract class ItemWriter extends ConnectorBase {

        static Builder builder() {
            return new com.cognite.client.servicesV1.AutoValue_ConnectorServiceV1_ItemWriter.Builder()
                    .setDuplicatesResponseParser(DEFAULT_DUPLICATES_RESPONSE_PARSER);
        }

        static ItemWriter of(CogniteClient client, RequestProvider requestProvider) {
            return ItemWriter.builder()
                    .setClient(client)
                    .setRequestExecutor(RequestExecutor.of(client.getHttpClient())
                            .withExecutor(client.getExecutorService())
                            .withMaxRetries(client.getClientConfig().getMaxRetries())
                            .withValidResponseCodes(ImmutableList.of(400, 409, 422)))
                    .setRequestProvider(requestProvider)
                    .build();
        }

        abstract Builder toBuilder();

        public abstract ResponseParser<String> getDuplicatesResponseParser();

        /**
         * Configures the duplicates response parser to use.
         *
         * The default duplicates response parser looks for duplicates at the {@code error.duplicates}
         * node in the response.
         * @param parser
         * @return
         */
        public ItemWriter withDuplicatesResponseParser(ResponseParser<String> parser) {
            return toBuilder().setDuplicatesResponseParser(parser).build();
        }

        /**
         * Executes an item-based write request.
         *
         * This method will block until the response is ready. The async version of this method is
         * {@code writeItemsAsync}.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public ResponseItems<String> writeItems(Request items) throws Exception {
            Preconditions.checkNotNull(items, "Input cannot be null.");
            return this.writeItemsAsync(items).join();
        }

        /**
         * Executes an item-based write request asynchronously.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public CompletableFuture<ResponseItems<String>> writeItemsAsync(Request items) throws Exception {
            Preconditions.checkNotNull(items, "Input cannot be null.");

            return getRequestExecutor().executeRequestAsync(getRequestProvider()
                    .withRequest(items)
                    .buildRequest(Optional.empty()))
                    .thenApply(responseBinary -> ResponseItems.of(DEFAULT_RESPONSE_PARSER, responseBinary)
                            .withDuplicateResponseParser(getDuplicatesResponseParser()));
        }

        @AutoValue.Builder
        abstract static class Builder extends ConnectorBase.Builder<Builder> {
            abstract Builder setDuplicatesResponseParser(ResponseParser<String> value);

            abstract ItemWriter build();
        }
    }

    /**
     * Reads file binaries from the Cognite API.
     */
    @AutoValue
    public static abstract class FileBinaryReader extends ConnectorBase {
        private static int DEFAULT_MAX_BATCH_SIZE_FILE_READ = 100;
        private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
        private final String loggingPrefix = "FileBinaryReader [" + randomIdString + "] -";
        private final ObjectMapper objectMapper = new ObjectMapper();

        static Builder builder() {
            return new com.cognite.client.servicesV1.AutoValue_ConnectorServiceV1_FileBinaryReader.Builder()
                    .setForceTempStorage(false)
                    .setRequestProvider(PostJsonRequestProvider.builder()
                            .setEndpoint("files/downloadlink")
                            .setRequest(Request.create())
                            .build());
        }

        static FileBinaryReader of(CogniteClient client) {
            return FileBinaryReader.builder()
                    .setClient(client)
                    .setRequestExecutor(RequestExecutor.of(client.getHttpClient())
                            .withExecutor(client.getExecutorService())
                            .withMaxRetries(client.getClientConfig().getMaxRetries())
                            .withValidResponseCodes(ImmutableList.of(400, 409, 422)))
                    .build();
        }

        abstract Builder toBuilder();
        abstract boolean isForceTempStorage();

        @Nullable
        abstract URI getTempStoragePath();

        /**
         * Forces the use of temp storage for all binaries--not just the >200MiB ones.
         *
         * @param enable
         * @return
         */
        public FileBinaryReader enableForceTempStorage(boolean enable) {
            return toBuilder().setForceTempStorage(enable).build();
        }

        /**
         * Sets the temporary storage path for storing large file binaries. If the binary is >200 MiB it will be
         * stored in temp storage instead of in memory.
         *
         * The following storage providers are supported:
         * - Google Cloud Storage. Specify the temp path as {@code gs://<my-storage-bucket>/<my-path>/}.
         * - Local (network) storage: {@code file://localhost/home/files/, file:///home/files/, file:///c:/temp/}
         *
         * @param path The URI to the temp storage
         * @return a {@FileBinaryReader} with temp storage configured.
         */
        public FileBinaryReader withTempStoragePath(URI path) {
            Preconditions.checkArgument(null != path,
                    "Temp storage path cannot be null or empty.");
            return toBuilder().setTempStoragePath(path).build();
        }

        /**
         * Executes an item-based request to get file binaries and blocks the thread until all files have been downloaded.
         *
         * We recommend reading a limited set of files per request, between 1 and 10--depending on the file size.
         * If the request fails due to missing items, it will return a single <code>ResultsItems</code>.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public List<ResponseItems<FileBinary>> readFileBinaries(Request items) throws Exception {
            return this.readFileBinariesAsync(items).join();
        }

        /**
         * Executes an item-based request to get file binaries asynchronously.
         *
         * We recommend reading a limited set of files per request, between 1 and 10--depending on the file size.
         * If the request fails due to missing items, it will return a single <code>ResultsItems</code>.
         *
         * @param items
         * @return
         * @throws Exception
         */
        public CompletableFuture<List<ResponseItems<FileBinary>>> readFileBinariesAsync(Request items)
                throws Exception {
            Preconditions.checkNotNull(items, "Input cannot be null.");
            LOG.debug(loggingPrefix + "Received {} file items requested to download.", items.getItems().size());

            PostJsonRequestProvider requestProvider = PostJsonRequestProvider.builder()
                    .setEndpoint("files/downloadlink")
                    .setRequest(Request.create())
                    .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                    .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                    .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                    .build();

            // Get the file download links
            ResponseItems<String> fileItemsResponse = getRequestExecutor()
                    .executeRequestAsync(requestProvider
                            .withRequest(items)
                            .buildRequest(Optional.empty()))
                    .thenApply(responseBinary -> ResponseItems.of(JsonItemResponseParser.create(), responseBinary))
                    .join();

            if (!fileItemsResponse.isSuccessful()) {
                LOG.warn(loggingPrefix + "Unable to retrieve the file download URLs");
                return CompletableFuture.completedFuture(ImmutableList.of(
                        ResponseItems.of(FileBinaryResponseParser.create(), fileItemsResponse.getResponseBinary()))
                );
            }

            List<String> fileItems = fileItemsResponse.getResultsItems();
            LOG.info(loggingPrefix + "Received {} file download URLs.", fileItems.size());

            // Start download the file binaries on separate threads
            List<CompletableFuture<ResponseItems<FileBinary>>> futuresList = new ArrayList<>();

            for (String fileItem : fileItems) {
                LOG.debug(loggingPrefix + "Building download request for file item: {}", fileItem);
                Map<String, Object> fileRequestItem = objectMapper.readValue(fileItem, new TypeReference<Map<String, Object>>(){});
                Preconditions.checkState(fileRequestItem != null && fileRequestItem.containsKey("downloadUrl"),
                        "File response does not contain a valid *downloadUrl* item.");
                Preconditions.checkState(fileRequestItem.containsKey("externalId") || fileRequestItem.containsKey("id"),
                        "File response does not contain a file id nor externalId.");

                // build file id and url
                String fileExternalId = (String) fileRequestItem.getOrDefault("externalId", "");
                long fileId = (Long) fileRequestItem.getOrDefault("id", -1);
                String downloadUrl = (String) fileRequestItem.get("downloadUrl");

                CompletableFuture<ResponseItems<FileBinary>> future = DownloadFileBinary
                        .downloadFileBinaryFromURL(downloadUrl, getTempStoragePath(), isForceTempStorage())
                        .thenApply(fileBinary -> {
                            // add the file id
                            if (!fileExternalId.isEmpty()) {
                                return fileBinary.toBuilder()
                                        .setExternalId(fileExternalId)
                                        .build();
                            } else {
                                return fileBinary.toBuilder()
                                        .setId(fileId)
                                        .build();
                            }
                        })
                        .thenApply(fileBinary -> {
                            // add a response items wrapper
                            return ResponseItems.of(FileBinaryResponseParser.create(), fileItemsResponse.getResponseBinary())
                                    .withResultsItemsList(ImmutableList.of(fileBinary));
                        });

                futuresList.add(future);
            }

            // Sync all downloads to a single future. It will complete when all the upstream futures have completed.
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(futuresList.toArray(
                    new CompletableFuture[futuresList.size()]));

            CompletableFuture<List<ResponseItems<FileBinary>>> responseItemsFuture = allFutures
                    .thenApply(future ->
                            futuresList.stream()
                                    .map(CompletableFuture::join)
                                    .collect(Collectors.toList())
                    );

            return responseItemsFuture;
        }

        @AutoValue.Builder
        abstract static class Builder extends ConnectorBase.Builder<Builder> {
            abstract Builder setForceTempStorage(boolean value);
            abstract Builder setTempStoragePath(URI value);

            abstract FileBinaryReader build();
        }
    }

    /**
     * Writes files (header + binary) to the Cognite API.
     *
     */
    @AutoValue
    public static abstract class FileWriter extends ConnectorBase {
        private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
        private final String loggingPrefix = "FileWriter [" + randomIdString + "] -";
        private final ObjectMapper objectMapper = new ObjectMapper();

        static Builder builder() {
            return new com.cognite.client.servicesV1.AutoValue_ConnectorServiceV1_FileWriter.Builder()
                    .setRequestProvider(PostJsonRequestProvider.builder()
                            .setEndpoint("files")
                            .setRequest(Request.create())
                            .build())
                    .setDeleteTempFile(true);
        }

        static FileWriter of(CogniteClient client) {
            return FileWriter.builder()
                    .setClient(client)
                    .setRequestExecutor(RequestExecutor.of(client.getHttpClient())
                            .withExecutor(client.getExecutorService())
                            .withMaxRetries(client.getClientConfig().getMaxRetries())
                            .withValidResponseCodes(ImmutableList.of(400)))
                    .setFileBinaryRequestExecutor(FileBinaryRequestExecutor.of(client.getHttpClient())
                            .withExecutor(client.getExecutorService())
                            .withMaxRetries(client.getClientConfig().getMaxRetries()))
                    .build();
        }

        abstract Builder toBuilder();

        abstract FileBinaryRequestExecutor getFileBinaryRequestExecutor();
        abstract boolean isDeleteTempFile();

        /**
         * Configure how to treat a temp blob after an upload. This setting only affects behavior when uploading
         * file binaries to the Cognite API--it has no effect on downloading file binaries.
         *
         * When set to {@code true}, the temp file (if present) will be removed after a successful upload. If the file
         * binary is memory-based (which is the default for small and medium sized files), this setting has no effect.
         *
         * When set to {@code false}, the temp file (if present) will not be deleted.
         *
         * The default setting is {@code true}.
         *
         * @param enable
         * @return
         */
        public FileWriter enableDeleteTempFile(boolean enable) {
            return toBuilder().setDeleteTempFile(enable).build();
        }

        /**
         * Writes file metadata and binaries and blocks the call until the file write completes.
         *
         * The <code>RequestParameter</code> input must contain a proto payload with a <code>FileContainer</code> object
         * which contains both the file metadata and the file binary.
         *
         * @param fileContainerRequest
         * @return
         * @throws Exception
         */
        public ResponseItems<String> writeFile(Request fileContainerRequest) throws Exception {
            return this.writeFileAsync(fileContainerRequest).join();
        }

        /**
         * Writes file metadata and binaries asynchronously.
         *
         * The <code>RequestParameter</code> input must contain a proto payload with a <code>FileContainer</code> object
         * which contains both the file metadata and the file binary.
         *
         * @param fileContainerRequest
         * @return
         * @throws Exception
         */
        public CompletableFuture<ResponseItems<String>> writeFileAsync(Request fileContainerRequest)
                throws Exception {
            final String uploadUrlKey = "uploadUrl";    // key for extracting the upload URL from the api json payload

            Preconditions.checkNotNull(fileContainerRequest, "Input cannot be null.");
            Preconditions.checkNotNull(fileContainerRequest.getProtoRequestBody(),
                    "No protobuf request body found.");
            Preconditions.checkArgument(fileContainerRequest.getProtoRequestBody() instanceof FileContainer,
                    "The protobuf request body is not of type FileContainer");
            Preconditions.checkArgument(!((FileContainer) fileContainerRequest.getProtoRequestBody())
                    .getFileMetadata().getName().isEmpty(),
                    "The request body must contain a file name in the file header section.");
            FileContainer fileContainer = (FileContainer) fileContainerRequest.getProtoRequestBody();
            boolean hasExtraAssetIds = false;

            LOG.info(loggingPrefix + "Received file container to write. Name: {}, Binary type: {}, Binary Size: {}MB, "
                    + "Binary URI: {}, Content lenght: {}, Number of asset links: {}, Number of metadata fields: {}",
                    fileContainer.getFileMetadata().getName(),
                    fileContainer.getFileBinary().getBinaryTypeCase().toString(),
                    String.format("%.3f", fileContainer.getFileBinary().getBinary().size() / (1024d * 1024d)),
                    fileContainer.getFileBinary().getBinaryUri(),
                    fileContainer.getFileBinary().getContentLength(),
                    fileContainer.getFileMetadata().getAssetIdsCount(),
                    fileContainer.getFileMetadata().getMetadataCount());

            FilesUploadRequestProvider requestProvider = FilesUploadRequestProvider.builder()
                    .setEndpoint("files")
                    .setRequest(Request.create())
                    .setSdkIdentifier(getClient().getClientConfig().getSdkIdentifier())
                    .setAppIdentifier(getClient().getClientConfig().getAppIdentifier())
                    .setSessionIdentifier(getClient().getClientConfig().getSessionIdentifier())
                    .build();

            // Build the file metadata request
            FileMetadata fileMetadata = fileContainer.getFileMetadata();
            FileMetadata fileMetadataExtraAssets = null;
            if (fileMetadata.getAssetIdsCount() > 1000) {
                LOG.info(loggingPrefix + "File contains >1k asset links. Will upload the file first and patch the assets afterwards");
                hasExtraAssetIds = true;
                fileMetadataExtraAssets = fileMetadata.toBuilder()
                        .clearAssetIds()
                        .addAllAssetIds(fileMetadata.getAssetIdsList().subList(1000, fileMetadata.getAssetIdsCount()))
                        .build();
                fileMetadata = fileMetadata.toBuilder()
                        .clearAssetIds()
                        .addAllAssetIds(fileMetadata.getAssetIdsList().subList(0, 1000))
                        .build();
            }

            Request postFileMetaRequest = fileContainerRequest
                    .withRequestParameters(FileParser.toRequestInsertItem(fileMetadata));

            // Post the file metadata and get the file upload links
            ResponseBinary fileUploadResponse = getRequestExecutor()
                    .executeRequestAsync(requestProvider
                            .withRequest(postFileMetaRequest)
                            .buildRequest(Optional.empty()))
                    .join();

            if (!fileUploadResponse.getResponse().isSuccessful()) {
                LOG.warn(loggingPrefix + "Unable to retrieve the file upload URLs");
                return CompletableFuture.completedFuture(
                        ResponseItems.of(JsonItemResponseParser.create(), fileUploadResponse)
                );
            }

            // Parse the upload response
            String jsonResponsePayload = fileUploadResponse.getResponseBodyBytes().toStringUtf8();
            Map<String, Object> fileUploadResponseItem = objectMapper
                    .readValue(jsonResponsePayload, new TypeReference<Map<String, Object>>(){});
            LOG.debug(loggingPrefix + "Posted file metadata for [{}]. Received file upload URL response.",
                    fileContainer.getFileMetadata().getName());

            Preconditions.checkState(fileUploadResponseItem.containsKey(uploadUrlKey),
                    "Unable to retrieve upload URL from the CogniteAPI: " + fileUploadResponseItem.toString());
            LOG.debug(loggingPrefix + "[{}] upload URL: {}",
                    fileContainer.getFileMetadata().getName(),
                    fileUploadResponseItem.getOrDefault(uploadUrlKey, "No upload URL"));

            // Start upload of the file binaries on a separate thread
            URL fileUploadURL = new URL((String) fileUploadResponseItem.get(uploadUrlKey));

            CompletableFuture<ResponseItems<String>> future;
            if (fileContainer.getFileBinary().getBinaryTypeCase() == FileBinary.BinaryTypeCase.BINARY
                    && fileContainer.getFileBinary().getBinary().isEmpty()) {
                LOG.warn(loggingPrefix + "Binary is empty for file {}. File externalId = [{}]. Will skip upload.",
                        fileContainer.getFileMetadata().getName(),
                        fileContainer.getFileMetadata().getExternalId());

                future = CompletableFuture.completedFuture(
                        ResponseItems.of(JsonResponseParser.create(), fileUploadResponse));
            } else {
                future = getFileBinaryRequestExecutor()
                        .enableDeleteTempFile(isDeleteTempFile())
                        .uploadBinaryAsync(fileContainer.getFileBinary(), fileUploadURL)
                        .thenApply(responseBinary -> {
                            long requestDuration = System.currentTimeMillis() - responseBinary.getResponse().sentRequestAtMillis();
                            LOG.info(loggingPrefix + "Upload complete for file [{}], size {}MB in {}s at {}mb/s",
                                    fileContainer.getFileMetadata().getName(),
                                    String.format("%.2f", fileContainer.getFileBinary().getBinary().size() / (1024d * 1024d)),
                                    String.format("%.2f", requestDuration / 1000d),
                                    String.format("%.2f",
                                            ((fileContainer.getFileBinary().getBinary().size() / (1024d * 1024d)) * 8)
                                                    / (requestDuration / 1000d))
                            );

                            return ResponseItems.of(JsonResponseParser.create(), fileUploadResponse);
                        });
            }

            // Patch the file with extra asset ids.
            if (hasExtraAssetIds) {
                LOG.info(loggingPrefix + "Start patching asset ids.");
                this.patchFileAssetLinks(fileMetadataExtraAssets, fileContainerRequest, loggingPrefix);
            }

            return future;
        }

        /*
         Write extra assets id links as separate updates. The api only supports 1k assetId links per file object
        per api request. If a file contains a large number of assetIds, we need to split them up into an initial
        file create/update (all the code in the writeFilesAsync) and subsequent update requests which add the remaining
        assetIds (this method).
         */
        private void patchFileAssetLinks(FileMetadata file,
                                         Request originalRequest,
                                         String loggingPrefix) throws Exception {
            // Split the assets into n request items with max 1k assets per request
            FileMetadata tempMeta = FileMetadata.newBuilder()
                    .setExternalId(file.getExternalId())
                    .addAllAssetIds(file.getAssetIdsList())
                    .build();

            List<FileMetadata> metaRequests = new ArrayList<>();
            while (tempMeta.getAssetIdsCount() > 1000) {
                metaRequests.add(tempMeta.toBuilder()
                        .clearAssetIds()
                        .addAllAssetIds(tempMeta.getAssetIdsList().subList(0,1000))
                        .build());
                tempMeta = tempMeta.toBuilder()
                        .clearAssetIds()
                        .addAllAssetIds(tempMeta.getAssetIdsList().subList(1000, tempMeta.getAssetIdsCount()))
                        .build();
            }
            metaRequests.add(tempMeta);

            ItemWriter updateWriter = ConnectorServiceV1.of(getClient())
                     .updateFileHeaders();

            for (FileMetadata metadata : metaRequests) {
                Request request = Request.create()
                        .withAuthConfig(originalRequest.getAuthConfig())
                        .withItems(ImmutableList.of(FileParser.toRequestAddAssetIdsItem(metadata)));

                ResponseItems<String> responseItems = updateWriter.writeItems(request);
                if (!responseItems.isSuccessful()) {
                    String message = loggingPrefix + "Failed to add assetIds for file. "
                            + responseItems.getResponseBodyAsString();
                    LOG.error(message);
                    throw new Exception(message);
                }
                LOG.info(loggingPrefix + "Posted {} assetIds as patch updates to the file.",
                        metadata.getAssetIdsCount());
            }
        }

        @AutoValue.Builder
        abstract static class Builder extends ConnectorBase.Builder<Builder> {
            abstract Builder setFileBinaryRequestExecutor(FileBinaryRequestExecutor value);
            abstract Builder setDeleteTempFile(boolean value);

            abstract FileWriter build();
        }
    }

    public static class DownloadFileBinary {
        final static Logger LOG = LoggerFactory.getLogger(DownloadFileBinary.class);
        final static String loggingPrefix = "DownloadFileBinary() - ";
        final static OkHttpClient client = new OkHttpClient.Builder()
                .connectionSpecs(Arrays.asList(ConnectionSpec.MODERN_TLS, ConnectionSpec.COMPATIBLE_TLS))
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS)
                .build();

        public static CompletableFuture<FileBinary> downloadFileBinaryFromURL(String downloadUrl) {
            return downloadFileBinaryFromURL(downloadUrl,
                    ConnectorConstants.DEFAULT_MAX_RETRIES,
                    null,
                    false);
        }

        public static CompletableFuture<FileBinary> downloadFileBinaryFromURL(String downloadUrl,
                                                                              URI tempStorageURI) {
            return downloadFileBinaryFromURL(downloadUrl,
                    ConnectorConstants.DEFAULT_MAX_RETRIES,
                    tempStorageURI,
                    false);
        }

        public static CompletableFuture<FileBinary> downloadFileBinaryFromURL(String downloadUrl,
                                                                              URI tempStorageURI,
                                                                              boolean forceTempStorage) {
            return downloadFileBinaryFromURL(downloadUrl,
                    ConnectorConstants.DEFAULT_MAX_RETRIES,
                    tempStorageURI,
                    forceTempStorage);
        }

        public static CompletableFuture<FileBinary> downloadFileBinaryFromURL(String downloadUrl,
                                                                              int maxRetries,
                                                                              @Nullable URI tempStorageURI,
                                                                              boolean forceTempStorage) {
            String loggingPrefix = DownloadFileBinary.loggingPrefix +  RandomStringUtils.randomAlphanumeric(5) + " - ";
            LOG.debug(loggingPrefix + "Download URL received: {}. Max retries: {}. Temp storage: {}. "
                    + "Force temp storage: {}",
                    downloadUrl,
                    maxRetries,
                    tempStorageURI,
                    forceTempStorage);
            Preconditions.checkArgument(!(null == tempStorageURI && forceTempStorage),
                    "Cannot force the use of temp storage without a valid temp storage URI.");
            HttpUrl url = HttpUrl.parse(downloadUrl);
            Preconditions.checkState(null != url, "Download URL not valid: " + downloadUrl);

            okhttp3.Request FileBinaryBuilder = new okhttp3.Request.Builder()
                    .url(url)
                    .build();

            FileBinaryRequestExecutor requestExecutor = FileBinaryRequestExecutor.of(client)
                    .withMaxRetries(maxRetries)
                    .enableForceTempStorage(forceTempStorage);

            if (null != tempStorageURI) {
                LOG.debug(loggingPrefix + "Temp storage URI detected: {}. "
                        + "Configuring request executor with temp storage.",
                        tempStorageURI);
                requestExecutor = requestExecutor.withTempStoragePath(tempStorageURI);
            }

            long startTimeMillies = Instant.now().toEpochMilli();

            CompletableFuture<FileBinary> future = requestExecutor.downloadBinaryAsync(FileBinaryBuilder)
                    .thenApply(fileBinary -> {
                        long requestDuration = System.currentTimeMillis() - startTimeMillies;
                        long contentLength = fileBinary.getContentLength();
                        double contentLengthMb = -1;
                        if (contentLength > 0) {
                            contentLengthMb = contentLength / (1024d * 1024d);
                        }
                        double downloadSpeed = -1;
                        if (contentLengthMb > 0) {
                            downloadSpeed = (contentLengthMb * 8) / (requestDuration / 1000d);
                        }
                        LOG.info(loggingPrefix + "Download complete for file, size {}MB in {}s at {}Mb/s",
                                String.format("%.2f", contentLengthMb),
                                String.format("%.2f", requestDuration / 1000d),
                                String.format("%.2f", downloadSpeed)
                        );
                        return fileBinary;
                    });
            return future;
        }
    }
}