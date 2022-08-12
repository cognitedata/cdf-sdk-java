package com.cognite.client;

import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.Transformation;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.TransformationJobsParser;
import com.cognite.client.servicesV1.parser.TransformationParser;
import com.cognite.client.servicesV1.request.PostJsonRequestProvider;
import com.google.auto.value.AutoValue;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@AutoValue
public abstract class TransformationJobs extends ApiBase {

    private static TransformationJobs.Builder builder() {
        return new AutoValue_TransformationJobs.Builder();
    }

    /**
     * Constructs a new {@link TransformationJobs} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static TransformationJobs of(CogniteClient client) {
        return TransformationJobs.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns {@link TransformationJobMetrics} representing TransformationJobMetrics api endpoints.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     client.transformation().jobs().metrics();
     * }
     * </pre>
     *
     * @see CogniteClient
     * @see CogniteClient#transformation()
     * @see Transformations#jobs()
     *
     * @return The TransformationJobMetrics api endpoints.
     */
    public TransformationJobMetrics metrics() {
        return TransformationJobMetrics.of(getClient());
    }

    /**
     * Start running the job
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Long internalID = 1L;
     *      Transformation.Job job =
     *              client.transformation().jobs().run(internalID);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Transformations/operation/runTransformation">API Reference - Run a transformation</a>
     *
     * @see CogniteClient
     * @see CogniteClient#transformation()
     * @see #run(Request)
     *
     * @param internalID Transformation Internal ID
     * @return the job details
     * @throws Exception
     */
    public Transformation.Job run(Long internalID) throws Exception {
        Request request = Request.create().withRootParameter("id", internalID);
        return run(request);
    }

    /**
     * Start running the job
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Long externalID = 1L;
     *      Transformation.Job job =
     *              client.transformation().jobs().run(externalID);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Transformations/operation/runTransformation">API Reference - Run a transformation</a>
     *
     * @see CogniteClient
     * @see CogniteClient#transformation()
     * @see #run(Request)
     *
     * @param externalID Transformation external ID
     * @return the job details
     * @throws Exception
     */
    public Transformation.Job run(String externalID) throws Exception {
        Request request = Request.create().withRootParameter("externalId", externalID);
        return run(request);
    }

    /**
     * Stop running job
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Long internalID = 1L;
     *      Boolean jobResult =
     *              client.transformation().jobs().cancel(internalID);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Transformations/operation/postApiV1ProjectsProjectTransformationsCancel">API Reference - Cancel a transformation</a>
     *
     * @see CogniteClient
     * @see CogniteClient#transformation()
     * @see #cancel(Request)
     *
     * @param internalID Transformation Internal ID
     * @return
     * @throws Exception
     */
    public Boolean cancel(Long internalID) throws Exception {
        Request request = Request.create().withRootParameter("id", internalID);
        return cancel(request);
    }

    /**
     * Stop running the job
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      Long externalID = 1L;
     *      Boolean jobResult =
     *              client.transformation().jobs().cancel(externalID);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Transformations/operation/postApiV1ProjectsProjectTransformationsCancel">API Reference - Cancel a transformation</a>
     *
     * @see CogniteClient
     * @see CogniteClient#transformation()
     * @see #cancel(Request)
     *
     * @param externalID Transformation external ID
     * @return
     * @throws Exception
     */
    public Boolean cancel(String externalID) throws Exception {
        Request request = Request.create().withRootParameter("externalId", externalID);
        return cancel(request);
    }

    /**
     * Returns all {@link Transformation.Job} objects.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<Transformation.Job> listResults = new ArrayList<>();
     *     client.transformation()
     *             .jobs()
     *             .list()
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Transformation-Jobs/operation/getTransformationJobs">API Reference - List jobs</a>
     *
     * @see #list(Request)
     * @see CogniteClient
     * @see CogniteClient#transformation()
     * @see Transformations#jobs()
     */
    public Iterator<List<Transformation.Job>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Returns all {@link Transformation.Job} object that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The Transformation.Job are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     List<Transformation.Job> listResults = new ArrayList<>();
     *     client.transformation()
     *             .jobs()
     *             .list(Request.create()
     *                                 .withRootParameter("transformationId", 1));
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Transformation-Jobs/operation/getTransformationJobs">API Reference - List jobs</a>
     *
     * @see #list(Request,String...)
     * @see CogniteClient
     * @see CogniteClient#transformation()
     * @see Transformations#jobs()
     *
     * @param requestParameters the filters to use for retrieving Transformation Job.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Transformation.Job>> list(Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link Transformation.Job} objects that matches the filters set in the {@link Request} for
     * the specified partitions. This method is intended for advanced use cases where you need direct control over the
     * individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Transformation.Job> listResults = new ArrayList<>();
     *      client.transformation()
     *              .jobs()
     *              .list(Request.create()
     *                             .withRootParameter("transformationId", 1),
     *                                  "1/8","2/8","3/8","4/8","5/8","6/8","7/8","8/8")
     *              .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Transformation-Jobs/operation/getTransformationJobs">API Reference - List jobs</a>
     *
     * @see #listJson(ResourceType,Request,String...)
     * @see CogniteClient
     * @see CogniteClient#transformation()
     * @see Transformations#jobs()
     *
     * @param requestParameters the filters to use for retrieving the Transformation Job.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Transformation.Job>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.TRANSFORMATIONS_JOBS, requestParameters, partitions), this::parseTransformationJobs);
    }

    /**
     *
     * Retrieve Transformation Jobs by {@code externalId / id}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *      List<Item> items = List.of(Item.newBuilder().setExternalId("1").build());
     *      List<Transformation.Job> retrievedTransformationJobs = client.transformation().jobs().retrieve(items);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Transformation-Jobs/operation/getTransformationJobsByIds">API Reference - Retrieve jobs by ids</a>
     *
     * @see #retrieveJson(ResourceType, Collection)
     * @see CogniteClient
     * @see CogniteClient#transformation()
     * @see Transformations#jobs()
     *
     * @param items The item(s) {@code externalId / id} to retrieve.
     * @return
     * @throws Exception
     */
    public List<Transformation.Job> retrieve(List<Item> items) throws Exception {
        return retrieveJson(ResourceType.TRANSFORMATIONS_JOBS, items).stream()
                .map(this::parseTransformationJobs)
                .collect(Collectors.toList());
    }


    private Transformation.Job run(Request request) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter runItemWriter = connector.runTransformationJobs(request);

        CompletableFuture<ResponseItems<String>> responseFuture =
                runItemWriter.writeItemsAsync(addAuthInfo(request));
        responseFuture.join();

        ResponseItems<String> response = responseFuture.get();
        if (!response.isSuccessful()) {
            String exceptionMessage = response.getResponseBodyAsString();
            LOG.debug("RUN TRANSFORMATION REQUEST FAILED: {}", exceptionMessage);
        }
        return parseTransformationJobs(response.getResponseBodyAsString());
    }

    private Boolean cancel(Request request) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter runItemWriter = connector.cancelTransformationJobs(request);

        CompletableFuture<ResponseItems<String>> responseFuture =
                runItemWriter.writeItemsAsync(addAuthInfo(request));
        responseFuture.join();

        ResponseItems<String> response = responseFuture.get();
        if (!response.isSuccessful()) {
            String exceptionMessage = response.getResponseBodyAsString();
            LOG.debug("RUN TRANSFORMATION REQUEST FAILED: {}", exceptionMessage);
            return false;
        }
        return true;
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Transformation.Job parseTransformationJobs(String json) {
        try {
            return TransformationJobsParser.parseTransformationJobs(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<TransformationJobs.Builder> {
        abstract TransformationJobs build();
    }

}
