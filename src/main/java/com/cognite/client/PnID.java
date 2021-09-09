/*
 * Copyright (c) 2021 Cognite AS
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

import com.cognite.client.dto.*;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.DiagramResponseParser;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.protobuf.Struct;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;


/**
 * This class represents the Cognite P&ID api endpoint
 *
 * It provides methods for detecting entities and creating interactive P&IDs.
 */
@AutoValue
public abstract class PnID extends ApiBase {

    private static Builder builder() {
        return new AutoValue_PnID.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(PnID.class);

    /**
     * Construct a new {@link PnID} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return The datasets api object.
     */
    public static PnID of(CogniteClient client) {
        return PnID.builder()
                .setClient(client)
                .build();
    }

    /**
     * Detect references to assets and files, etc. from a P&ID and annotate them with bounding boxes.
     * The P&ID must be a single-page PDF file or an image with JPEG, PNG or TIFF format.
     *
     * The detection will perform complete matches (i.e. partial matches are not accepted).
     *
     * @param files The P&IDs to process.
     * @param entities The entities to use for matching.
     * @param searchField The entity attribute to use for string matching.
     * @param convertToInteractive If set to {@code true} then an interactive P&ID (SVG) will be included in the results.
     * @return The results from the detect annotations job(s).
     * @throws Exception
     */
    public List<PnIDResponse> detectAnnotationsPnID(Collection<Item> files,
                                                    Collection<Struct> entities,
                                                    String searchField,
                                                    boolean convertToInteractive) throws Exception {
        return detectAnnotationsPnID(files, entities, searchField, false, 2, convertToInteractive);
    }

    /**
     * Detect references to assets and files, etc. from a P&ID and annotate them with bounding boxes.
     * The P&ID must be a single-page PDF file or an image with JPEG, PNG or TIFF format.
     *
     * @param files The P&IDs to process.
     * @param entities The entities to use for matching.
     * @param searchField The entity attribute to use for string matching.
     * @param partialMatch If set to {@code true}, use partial matching
     * @param minTokens The minimum number of tokens (consecutive letters/numbers) required for a match.
     * @param convertToInteractive If set to {@code true} then an interactive P&ID (SVG) will be included in the results.
     * @return The results from the detect annotations job(s).
     * @throws Exception
     */
    public List<PnIDResponse> detectAnnotationsPnID(Collection<Item> files,
                                                    Collection<Struct> entities,
                                                    String searchField,
                                                    boolean partialMatch,
                                                    int minTokens,
                                                    boolean convertToInteractive) throws Exception {
        final String loggingPrefix = "detectAnnotationsPnID() - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        Preconditions.checkNotNull(files, loggingPrefix + "Files cannot be null.");
        Preconditions.checkNotNull(entities, loggingPrefix + "Entities cannot be null.");
        Preconditions.checkNotNull(searchField, loggingPrefix + "Search field cannot be null.");

        LOG.debug(loggingPrefix + "Received {} files to process for {} entities. Search field: {}, partial match: {}, "
                + "min tokens: {}, convert to SVG: {}",
                files.size(),
                entities.size(),
                searchField,
                partialMatch,
                minTokens,
                convertToInteractive);

        if (files.isEmpty() || entities.isEmpty()) {
            LOG.info(loggingPrefix + "Files list or entities list is empty. Will ignore the request and return an empty response.");
            return Collections.emptyList();
        }

        // Build the baseline request.
        Request detectAnnotations = Request.create()
                .withRootParameter("entities", entities)
                .withRootParameter("searchField", searchField)
                .withRootParameter("partialMatch", partialMatch)
                .withRootParameter("minTokens", minTokens);

        List<Request> requestBatches = new ArrayList<>();
        for (Item file : files) {
            if (file.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                requestBatches.add(detectAnnotations.withRootParameter("fileExternalId", file.getExternalId()));
            } else if (file.getIdTypeCase() == Item.IdTypeCase.ID) {
                requestBatches.add(detectAnnotations.withRootParameter("fileId", file.getId()));
            }
        }

        return detectAnnotationsPnID(requestBatches, convertToInteractive);
    }


    /**
     * Detect references to assets and files, etc. from a P&ID and annotate them with bounding boxes.
     * The P&ID must be a single-page PDF file or an image with JPEG, PNG or TIFF format.
     *
     * All input parameters are provided via the request object.
     * @param requests Input parameters for the detect annotations job(s).
     * @param convertToInteractive If set to {@code true} then an interactive P&ID (SVG) will be included in the results.
     * @return The results from the detect annotations job(s).
     * @throws Exception
     */
    public List<PnIDResponse> detectAnnotationsPnID(Collection<Request> requests,
                                                    boolean convertToInteractive) throws Exception {
        final String loggingPrefix = "detectAnnotationsPnID() - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        Preconditions.checkNotNull(requests, loggingPrefix + "Requests cannot be null.");
        Instant startInstant = Instant.now();
        LOG.debug(loggingPrefix + "Received {} detect annotations requests.",
                requests.size());

        if (requests.isEmpty()) {
            LOG.info(loggingPrefix + "No items specified in the request. Will skip the detect annotations request.");
            return Collections.emptyList();
        }

        ItemReader<String> annotationsReader = getClient().getConnectorService().detectAnnotationsPnid();

        List<CompletableFuture<ResponseItems<String>>> resultFutures = new ArrayList<>();
        for (Request request : requests) {
            resultFutures.add(annotationsReader.getItemsAsync(addAuthInfo(request)));
        }
        LOG.info(loggingPrefix + "Submitted {} detect annotations jobs within a duration of {}.",
                requests.size(),
                Duration.between(startInstant, Instant.now()).toString());

        // Sync all downloads to a single future. It will complete when all the upstream futures have completed.
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(resultFutures.toArray(
                new CompletableFuture[resultFutures.size()]));
        // Wait until the uber future completes.
        allFutures.join();

        // Collect the response items
        List<String> responseItems = new ArrayList<>();
        for (CompletableFuture<ResponseItems<String>> responseItemsFuture : resultFutures) {
            if (!responseItemsFuture.join().isSuccessful()) {
                // something went wrong with the request
                String message = loggingPrefix + "Detect annotations job failed: "
                        + responseItemsFuture.join().getResponseBodyAsString();
                LOG.error(message);
                throw new Exception(message);
            }
            responseItemsFuture.join().getResultsItems().forEach(result -> responseItems.add(result));
        }

        LOG.info(loggingPrefix + "Completed detecting annotations for {} files within a duration of {}.",
                responseItems.size(),
                Duration.between(startInstant, Instant.now()).toString());

        List<PnIDResponse> results = responseItems.stream()
                .map(this::parsePnIDAnnotationResult)
                .collect(Collectors.toList());

        if (convertToInteractive) {
            results = convertPnID(results, false);
            LOG.info(loggingPrefix + "Completed converting {} annotated files within a duration of {}.",
                    responseItems.size(),
                    Duration.between(startInstant, Instant.now()).toString());
        }

        return results;
    }

    /**
     * Convert a single-page P&ID in PDF format to an interactive SVG where the provided annotations are highlighted.
     *
     * @param annotationsList Original P&ID with annotations.
     * @param grayscale Set to {@code true} to return the SVG in greyscale (reduces the file size).
     * @return Annotations together with an interactive SVG binary
     * @throws Exception
     */
    public List<PnIDResponse> convertPnID(Collection<PnIDResponse> annotationsList, boolean grayscale) throws Exception {
        final String loggingPrefix = "convertPnID() - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        Preconditions.checkNotNull(annotationsList, loggingPrefix + "Requests cannot be null.");
        Instant startInstant = Instant.now();
        LOG.debug(loggingPrefix + "Received {} convert P&ID requests.",
                annotationsList.size());

        if (annotationsList.isEmpty()) {
            LOG.info(loggingPrefix + "No items specified in the request. Will skip the convert P&ID request.");
            return Collections.emptyList();
        }

        ItemReader<String> convertReader = getClient().getConnectorService().convertPnid();

        Map<CompletableFuture<PnIDResponse>, PnIDResponse> futuresMap = new HashMap();
        for (PnIDResponse annotations : annotationsList) {
            Request interactiveFilesRequest = Request.create()
                    .withRootParameter("items", annotations.getItemsList())
                    .withRootParameter("grayscale", grayscale);

            if (annotations.hasFileExternalId()) {
                interactiveFilesRequest = interactiveFilesRequest
                        .withRootParameter("fileExternalId", annotations.getFileExternalId());
            } else {
                interactiveFilesRequest = interactiveFilesRequest
                        .withRootParameter("fileId", annotations.getFileId());
            }

            CompletableFuture<PnIDResponse> future = convertReader.getItemsAsync(addAuthInfo(interactiveFilesRequest))
                    .thenApply(stringResponseItems -> {
                        if (!stringResponseItems.isSuccessful()) {
                            LOG.error(loggingPrefix + "The convert api job did not complete successfully. Response body: {}",
                                    stringResponseItems.getResponseBodyAsString());
                            throw new CompletionException(
                                    new Throwable("The convert api job did not complete successfully. Response body: "
                                            + stringResponseItems.getResponseBodyAsString()));
                        }

                        try {
                            ConvertResponse convertResponse =
                                    parseConvertResult(stringResponseItems.getResultsItems().get(0));

                            // download the binaries
                            CompletableFuture<FileBinary> svgResponse = null;
                            CompletableFuture<FileBinary> pngResponse = null;
                            if (convertResponse.hasSvgUrl()) {
                                svgResponse = ConnectorServiceV1.DownloadFileBinary
                                        .downloadFileBinaryFromURL(convertResponse.getSvgUrl());
                            }
                            if (convertResponse.hasPngUrl()) {
                                pngResponse = ConnectorServiceV1.DownloadFileBinary
                                        .downloadFileBinaryFromURL(convertResponse.getPngUrl());
                            }

                            // add the binaries to a response object
                            PnIDResponse.Builder responseBuilder = PnIDResponse.newBuilder();
                            if (null != svgResponse) {
                                LOG.debug(loggingPrefix + "Found SVG. Adding to response object");
                                responseBuilder.setSvgBinary(svgResponse.join().getBinary());
                            }
                            if (null != pngResponse) {
                                LOG.debug(loggingPrefix + "Found PNG. Adding to response object");
                                responseBuilder.setPngBinary(pngResponse.join().getBinary());
                            }

                            return responseBuilder.build();

                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });

            futuresMap.put(future, annotations);
        }

        LOG.info(loggingPrefix + "Submitted {} convert P&ID jobs within a duration of {}.",
                annotationsList.size(),
                Duration.between(startInstant, Instant.now()).toString());

        // Wait for all requests futures to complete
        List<CompletableFuture<PnIDResponse>> futureList = new ArrayList<>();
        futuresMap.keySet().forEach(future -> futureList.add(future));
        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        allFutures.join(); // Wait for all futures to complete

        // Merge the downloaded binaries with the original annotations
        List<PnIDResponse> results = new ArrayList<>();
        for (Map.Entry<CompletableFuture<PnIDResponse>, PnIDResponse> entry : futuresMap.entrySet()) {
            results.add(entry.getValue().toBuilder()
                    .mergeFrom(entry.getKey().join())
                    .build());
        }

        LOG.info(loggingPrefix + "Completed convert P&ID for {} files within a duration of {}.",
                annotationsList.size(),
                Duration.between(startInstant, Instant.now()).toString());

        return results;
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private PnIDResponse parsePnIDAnnotationResult(String json) {
        try {
            return DiagramResponseParser.ParsePnIDAnnotationResponse(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private ConvertResponse parseConvertResult(String json) {
        try {
            return DiagramResponseParser.ParsePnIDConvertResponse(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract PnID build();
    }
}
