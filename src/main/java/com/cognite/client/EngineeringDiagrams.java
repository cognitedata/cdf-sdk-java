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

import com.cognite.client.dto.ConvertResponse;
import com.cognite.client.dto.DiagramResponse;
import com.cognite.client.dto.FileBinary;
import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.DiagramResponseParser;
import com.cognite.client.util.Partition;
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
 * This class represents the Cognite engineering api endpoint
 *
 * It provides methods for detecting entities, annotating and creating interactive diagrams.
 */
@AutoValue
public abstract class EngineeringDiagrams extends ApiBase {

    private static Builder builder() {
        return new AutoValue_EngineeringDiagrams.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(EngineeringDiagrams.class);

    /**
     * Construct a new {@link EngineeringDiagrams} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return The datasets api object.
     */
    public static EngineeringDiagrams of(CogniteClient client) {
        return EngineeringDiagrams.builder()
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
    public List<DiagramResponse> detectAnnotationsDiagrams(Collection<Item> files,
                                                    Collection<Struct> entities,
                                                    String searchField,
                                                    boolean convertToInteractive) throws Exception {
        return detectAnnotationsDiagrams(files, entities, searchField, false, 2, convertToInteractive);
    }

    /**
     * Detect references to assets and files, etc. from an engineering diagram and annotate them with bounding boxes.
     * The engineering diagram must be a PDF file or an image with JPEG, PNG or TIFF format.
     *
     * @param files The engineering diagram files to process.
     * @param entities The entities to use for matching.
     * @param searchField The entity attribute to use for string matching.
     * @param partialMatch If set to {@code true}, use partial matching
     * @param minTokens The minimum number of tokens (consecutive letters/numbers) required for a match.
     * @param convertToInteractive If set to {@code true} then an interactive P&ID (SVG) will be included in the results.
     * @return The results from the detect annotations job(s).
     * @throws Exception
     */
    public List<DiagramResponse> detectAnnotationsDiagrams(Collection<Item> files,
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

        // Build the item batches
        List<List<Item>> itemBatchesList = Partition.ofSize(new ArrayList<>(files), 2);

        List<Request> requestBatches = new ArrayList<>();
        for (List<Item> fileBatch : itemBatchesList) {
            List<Map<String, Object>> items = new ArrayList<>();
            fileBatch.forEach(file -> {
                if (file.getIdTypeCase() == Item.IdTypeCase.EXTERNAL_ID) {
                    items.add(Map.of("fileExternalId", file.getExternalId()));
                } else if (file.getIdTypeCase() == Item.IdTypeCase.ID) {
                    items.add(Map.of("fileId", file.getId()));
                }
            });

            requestBatches.add(detectAnnotations.withItems(items));
        }

        return detectAnnotationsDiagrams(requestBatches, convertToInteractive);
    }


    /**
     * Detect references to assets and files, etc. from an engineering diagram and annotate them with bounding boxes.
     * The P&ID must be a single-page PDF file or an image with JPEG, PNG or TIFF format.
     *
     * All input parameters are provided via the request object.
     * @param requests Input parameters for the detect annotations job(s).
     * @param convertToInteractive If set to {@code true} then an interactive P&ID (SVG) will be included in the results.
     * @return The results from the detect annotations job(s).
     * @throws Exception
     */
    public List<DiagramResponse> detectAnnotationsDiagrams(Collection<Request> requests,
                                                    boolean convertToInteractive) throws Exception {
        final String loggingPrefix = "detectAnnotationsDiagrams() - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        Preconditions.checkNotNull(requests, loggingPrefix + "Requests cannot be null.");
        Instant startInstant = Instant.now();
        LOG.debug(loggingPrefix + "Received {} detect annotations requests.",
                requests.size());

        if (requests.isEmpty()) {
            LOG.info(loggingPrefix + "No items specified in the request. Will skip the detect annotations request.");
            return Collections.emptyList();
        }

        ItemReader<String> annotationsReader = getClient().getConnectorService().detectAnnotationsDiagrams();

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
            responseItems.addAll(responseItemsFuture.join().getResultsItems());
        }

        LOG.info(loggingPrefix + "Completed detecting annotations for {} files within a duration of {}.",
                responseItems.size(),
                Duration.between(startInstant, Instant.now()).toString());

        List<DiagramResponse> results = responseItems.stream()
                .map(this::parseDiagramAnnotationResult)
                .collect(Collectors.toList());

        if (convertToInteractive) {
            results = convertDiagrams(results, false);
            LOG.info(loggingPrefix + "Completed converting {} annotated files within a duration of {}.",
                    responseItems.size(),
                    Duration.between(startInstant, Instant.now()).toString());
        }

        return results;
    }

    /**
     * Convert an engineering diagram (P&ID) in PDF format to an interactive SVG where the provided
     * annotations are highlighted.
     *
     * @param annotationsList Original engineering diagram with annotations.
     * @param grayscale Set to {@code true} to return the SVG/PNG in greyscale (reduces the file size).
     * @return Annotations together with an interactive SVG/PNG binary
     * @throws Exception
     */
    public List<DiagramResponse> convertDiagrams(Collection<DiagramResponse> annotationsList, boolean grayscale) throws Exception {
        final String loggingPrefix = "convertDiagrams() - batch: " + RandomStringUtils.randomAlphanumeric(6) + " - ";
        Preconditions.checkNotNull(annotationsList, loggingPrefix + "Requests cannot be null.");
        Instant startInstant = Instant.now();
        LOG.debug(loggingPrefix + "Received {} convert diagrams requests.",
                annotationsList.size());

        if (annotationsList.isEmpty()) {
            LOG.info(loggingPrefix + "No items specified in the request. Will skip the convert diagrams request.");
            return Collections.emptyList();
        }

        ItemReader<String> convertReader = getClient().getConnectorService().convertDiagrams();

        // Build the collection of "items" for the convert request(s) and the lookup maps for the original annotations
        Map<Long, DiagramResponse> internalIdMap = new HashMap<>();
        Map<String, DiagramResponse> externalIdMap = new HashMap<>();
        List<Map<String, Object>> itemsList = new ArrayList<>();
        for (DiagramResponse annotations : annotationsList) {
            Map<String, Object> item = new HashMap<>();

            // each item must have an origin file reference
            if (annotations.hasFileExternalId()) {
                item.put("fileExternalId", annotations.getFileExternalId());
                externalIdMap.put(annotations.getFileExternalId(), annotations);
            } else {
                item.put("fileId", annotations.getFileId());
                internalIdMap.put(annotations.getFileId(), annotations);
            }

            // add the annotations for the file
            item.put("annotations", annotations.getItemsList());
        }

        // Partition into batches per request
        List<List<Map<String, Object>>> itemsBatchList = Partition.ofSize(itemsList, 2);

        List<CompletableFuture<List<DiagramResponse>>> futuresList = new ArrayList<>();
        for (List<Map<String, Object>> itemsBatch : itemsBatchList) {
            Request interactiveFilesRequest = Request.create()
                    .withRootParameter("items", itemsBatch)
                    .withRootParameter("grayscale", grayscale);

            CompletableFuture<List<DiagramResponse>> future = convertReader.getItemsAsync(addAuthInfo(interactiveFilesRequest))
                    .thenApply(stringResponseItems -> {
                        if (!stringResponseItems.isSuccessful()) {
                            LOG.error(loggingPrefix + "The convert api job did not complete successfully. Response body: {}",
                                    stringResponseItems.getResponseBodyAsString());
                            throw new CompletionException(
                                    new Throwable("The convert api job did not complete successfully. Response body: "
                                            + stringResponseItems.getResponseBodyAsString()));
                        }

                        List<DiagramResponse> responseList = new ArrayList<>();
                        try {
                            for (String responseItem : stringResponseItems.getResultsItems()) {
                                ConvertResponse convertResponse = parseConvertResult(responseItem);
                                DiagramResponse.Builder diagramResponseBuilder = DiagramResponse.newBuilder();
                                if (convertResponse.hasFileExternalId()) {
                                    diagramResponseBuilder.setFileExternalId(convertResponse.getFileExternalId());
                                }
                                if (convertResponse.hasFileId()) {
                                    diagramResponseBuilder.setFileId(convertResponse.getFileId());
                                }

                                // download the binaries
                                for (ConvertResponse.Result result : convertResponse.getResultsList()) {
                                    DiagramResponse.ConvertResult.Builder convertResponseBuilder =
                                            DiagramResponse.ConvertResult.newBuilder();

                                    convertResponseBuilder.setPage(result.getPage());
                                    if (result.hasErrorMessage()) {
                                        convertResponseBuilder.setErrorMessage(result.getErrorMessage());
                                    } else {
                                        // we have successfully converted the page to SVG/PNG
                                        CompletableFuture<FileBinary> svgResponse = null;
                                        CompletableFuture<FileBinary> pngResponse = null;
                                        if (result.hasSvgUrl()) {
                                            svgResponse = ConnectorServiceV1.DownloadFileBinary
                                                    .downloadFileBinaryFromURL(result.getSvgUrl());
                                        }
                                        if (result.hasPngUrl()) {
                                            pngResponse = ConnectorServiceV1.DownloadFileBinary
                                                    .downloadFileBinaryFromURL(result.getPngUrl());
                                        }

                                        if (null != svgResponse) {
                                            LOG.debug(loggingPrefix + "Found SVG. Adding to response object");
                                            convertResponseBuilder.setSvgBinary(svgResponse.join().getBinary());
                                        }
                                        if (null != pngResponse) {
                                            LOG.debug(loggingPrefix + "Found PNG. Adding to response object");
                                            convertResponseBuilder.setPngBinary(pngResponse.join().getBinary());
                                        }
                                    }
                                    diagramResponseBuilder.addConvertResults(convertResponseBuilder.build());
                                }
                                responseList.add(diagramResponseBuilder.build());
                            }
                            return responseList;

                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });

            futuresList.add(future);
        }

        LOG.info(loggingPrefix + "Submitted {} convert P&ID jobs within a duration of {}.",
                annotationsList.size(),
                Duration.between(startInstant, Instant.now()).toString());

        // Wait for all requests futures to complete
        CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(futuresList.toArray(new CompletableFuture[futuresList.size()]));
        allFutures.join(); // Wait for all futures to complete

        // Merge the downloaded binaries with the original annotations
        List<DiagramResponse> results = new ArrayList<>();
        for (CompletableFuture<List<DiagramResponse>> future : futuresList) {
            for (DiagramResponse response : future.join()) {
                if (response.hasFileExternalId()) {
                    results.add(response.toBuilder()
                            .mergeFrom(externalIdMap.get(response.getFileExternalId()))
                            .build());
                } else if (response.hasFileId()) {
                    results.add(response.toBuilder()
                            .mergeFrom(internalIdMap.get(response.getFileId()))
                            .build());
                } else {
                    String message = loggingPrefix + "Response from the convert operation does not contain any file externalId or id.";
                    LOG.error(message);
                    throw new Exception(message);
                }
            }
        }

        LOG.info(loggingPrefix + "Completed convert engineering diagrams for {} files within a duration of {}.",
                annotationsList.size(),
                Duration.between(startInstant, Instant.now()).toString());

        return results;
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private DiagramResponse parseDiagramAnnotationResult(String json) {
        try {
            return DiagramResponseParser.ParseDiagramAnnotationResponse(json);
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
            return DiagramResponseParser.ParseDiagramConvertResponse(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract EngineeringDiagrams build();
    }
}
