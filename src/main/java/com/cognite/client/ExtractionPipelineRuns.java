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

import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.ExtractionPipelineRun;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.ExtractionPipelineParser;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite extraction pipelines api endpoint.
 *
 * It provides methods for reading and writing {@link ExtractionPipelineRuns}.
 */
@AutoValue
public abstract class ExtractionPipelineRuns extends ApiBase {

    private static Builder builder() {
        return new AutoValue_ExtractionPipelineRuns.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(ExtractionPipelineRuns.class);

    /**
     * Constructs a new {@link ExtractionPipelineRuns} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static ExtractionPipelineRuns of(CogniteClient client) {
        return ExtractionPipelineRuns.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link ExtractionPipelineRun} objects.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<ExtractionPipelineRun> listResults = new ArrayList<>();
     *     client.extractionPipelines()
     *             .list()
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * @see #list(Request)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     */
    public Iterator<List<ExtractionPipelineRun>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Returns all {@link ExtractionPipelineRun} objects that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<ExtractionPipelineRun> listResults = new ArrayList<>();
     *      client.extractionPipelines()
     *              .list(Request.create()
     *                             .withFilterParameter("source", "source"))
     *              .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * @see #list(Request,String...)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     *
     * @param requestParameters the filters to use for retrieving the assets.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ExtractionPipelineRun>> list(Request requestParameters) throws Exception {
        /*
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
         */

        // The extraction pipeline API endpoint does not support partitions (yet). Therefore no partitions are used here
        // in the list implementation.

        return list(requestParameters, new String[0]);
    }

    /**
     * Returns all {@link ExtractionPipelineRun} objects that matches the filters set in the {@link Request} for the
     * specified partitions. This is method is intended for advanced use cases where you need direct control over
     * the individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<ExtractionPipelineRun> listResults = new ArrayList<>();
     *      client.extractionPipelines()
     *              .list(Request.create()
     *                             .withFilterParameter("statuses", List.of("success", "failure", "seen")),
     *                                  "1/8","2/8","3/8","4/8","5/8","6/8","7/8","8/8")
     *              .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * @see #listJson(ResourceType,Request,String...)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     *
     * @param requestParameters the filters to use for retrieving the assets.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<ExtractionPipelineRun>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.EXTRACTION_PIPELINE_RUN, requestParameters, partitions),
                this::parseExtractionPipelineRun);
    }

    /**
     * Creates a set of {@link ExtractionPipelineRun} objects.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<ExtractionPipelineRun> runs = // List of Extraction Pipeline Run;
     *      client.extractionPipelines().runs().upsert(runs);
     * }
     * </pre>
     *
     * @see UpsertItems#create(List)
     * @see CogniteClient
     * @see CogniteClient#extractionPipelines()
     * @see ExtractionPipelines#runs()
     *
     * @param runs The extraction pipelines to create.
     * @return The created extraction pipelines.
     * @throws Exception
     */
    public List<ExtractionPipelineRun> create(List<ExtractionPipelineRun> runs) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeExtractionPipelineRuns();
        //ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateExtractionPipelines();

        UpsertItems<ExtractionPipelineRun> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildAuthConfig())
                .withIdFunction(this::getExtractionPipelineId)
                .withMaxBatchSize(1);

        return upsertItems.create(runs).stream()
                .map(this::parseExtractionPipelineRun)
                .collect(Collectors.toList());
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private ExtractionPipelineRun parseExtractionPipelineRun(String json) {
        try {
            return ExtractionPipelineParser.parseExtractionPipelineRun(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(ExtractionPipelineRun item) {
        try {
            return ExtractionPipelineParser.toRequestInsertItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of an extraction pipeline. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getExtractionPipelineId(ExtractionPipelineRun item) {
        if (item.hasExternalId()) {
            return Optional.of(item.getExternalId());
        } else if (item.hasId()) {
            return Optional.of(String.valueOf(item.getId()));
        } else {
            return Optional.<String>empty();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract ExtractionPipelineRuns build();
    }
}
