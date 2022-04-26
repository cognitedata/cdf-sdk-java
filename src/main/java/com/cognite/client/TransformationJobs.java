package com.cognite.client;

import com.cognite.client.dto.Transformation;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.TransformationJobsParser;
import com.cognite.client.servicesV1.parser.TransformationParser;
import com.cognite.client.servicesV1.request.PostJsonRequestProvider;
import com.google.auto.value.AutoValue;

import java.util.concurrent.CompletableFuture;

@AutoValue
public abstract class TransformationJobs extends ApiBase {

    private static TransformationJobs.Builder builder() {
        return new AutoValue_TransformationJobs.Builder();
    }

    /**
     * Constructs a new {@link Transformations} object using the provided client configuration.
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
     * Start running the job
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
     * @param externalID Transformation external ID
     * @return
     * @throws Exception
     */
    public Boolean cancel(String externalID) throws Exception {
        Request request = Request.create().withRootParameter("externalId", externalID);
        return cancel(request);
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
