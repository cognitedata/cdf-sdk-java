package com.cognite.client;

import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.Transformation;
import com.cognite.client.servicesV1.parser.ThreeDModelRevisionParser;
import com.cognite.client.servicesV1.parser.TransformationJobMetricsParser;
import com.google.auto.value.AutoValue;

import java.util.Iterator;
import java.util.List;

@AutoValue
public abstract class TransformationJobMetrics extends ApiBase {

    private static TransformationJobMetrics.Builder builder() {
        return new AutoValue_TransformationJobMetrics.Builder();
    }

    /**
     * Constructs a new {@link TransformationJobMetrics} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static TransformationJobMetrics of(CogniteClient client) {
        return TransformationJobMetrics.builder()
                .setClient(client)
                .build();
    }

    /**
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Integer jobId = 1;
     *     List<Transformation.Job.Metric> listResults = new ArrayList<>();
     *     client.transformation()
     *             .jobs()
     *             .metrics()
     *             .list(jobId)
     *             .forEachRemaining(listResults::addAll);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/Transformation-Jobs/operation/getTransformationJobsMetrics">API Reference - List job metrics by job id</a>
     *
     * @see #buildPartitionsList(int)
     * @see #listJson(ResourceType,Request,String...)
     * @see CogniteClient
     * @see CogniteClient#transformation()
     * @see Transformations#jobs()
     * @see TransformationJobs#metrics()
     *
     * @param jobId Transformation Job ID
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Transformation.Job.Metric>> list(Integer jobId) throws Exception {
        Request request = Request.create().withRootParameter("jobId", jobId);
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());
        return AdapterIterator.of(listJson(ResourceType.TRANSFORMATIONS_JOB_METRICS, request, partitions.toArray(new String[0])), this::parseTransformationJobMetrics);
    }

    private Transformation.Job.Metric parseTransformationJobMetrics(String json) {
        try {
            return TransformationJobMetricsParser.parseTransformationJobMetrics(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<TransformationJobMetrics.Builder> {
        abstract TransformationJobMetrics build();
    }
}
