package com.cognite.client;

import com.cognite.client.config.ResourceType;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.Transformation;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.TransformationJobsParser;
import com.cognite.client.servicesV1.parser.TransformationParser;
import com.cognite.client.servicesV1.parser.TransformationSchedulesParser;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@AutoValue
public abstract class TransformationSchedules extends ApiBase {

    private static TransformationSchedules.Builder builder() {
        return new AutoValue_TransformationSchedules.Builder();
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
    public static TransformationSchedules of(CogniteClient client) {
        return TransformationSchedules.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link Transformation.Schedule} objects.
     *
     * @see #list(Request)
     */
    public Iterator<List<Transformation.Schedule>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Returns all {@link Transformation.Schedule} object that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The Transformation.Job are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param requestParameters the filters to use for retrieving Transformation Job.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Transformation.Schedule>> list(Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link Transformation.Schedule} objects that matches the filters set in the {@link Request} for
     * the specified partitions. This method is intended for advanced use cases where you need direct control over the
     * individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param requestParameters the filters to use for retrieving the Transformation Job.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Transformation.Schedule>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.TRANSFORMATIONS_SCHEDULES, requestParameters, partitions), this::parseTransformationSchedules);
    }

    /**
     * Creates or updates a set of {@link Transformation.Schedule} objects.
     * <p>
     * If it is a new {@link Transformation.Schedule} object (based on {@code id / externalId}, then it will be created.
     * <p>
     * If an {@link Transformation.Schedule} object already exists in Cognite Data Fusion, it will be updated. The update behavior
     * is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * @param schedules The Transformation.Schedule to upsert.
     * @return The upserted Transformation.Schedule.
     * @throws Exception
     */
    public List<Transformation.Schedule> schedule(List<Transformation.Schedule> schedules) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeTransformationSchedules();
        ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateTransformationSchedules();

        UpsertItems<Transformation.Schedule> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildAuthConfig())
                .withUpdateItemWriter(updateItemWriter)
                .withUpdateMappingFunction(this::toRequestUpdateItem)
                .withIdFunction(this::getTransformationScheduleId);

        if (getClient().getClientConfig().getUpsertMode() == UpsertMode.REPLACE) {
            upsertItems = upsertItems.withUpdateMappingFunction(this::toRequestReplaceItem);
        }

        return upsertItems.upsertViaCreateAndUpdate(schedules).stream()
                .map(this::parseTransformationSchedules)
                .collect(Collectors.toList());
    }

    /**
     * Delete schedules
     *
     * @param items
     * @return The item(s) {@code externalId / id} to delete.
     * @throws Exception
     */
    public Boolean unSchedule(List<Item> items) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteTransformationSchedules();

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildAuthConfig())
                .addParameter("ignoreUnknownIds", true);

        return deleteItems.deleteItems(items).isEmpty() ? false : true;
    }

    /**
     * Retrieve Transformation.Schedule by {@code externalId / id}.
     *
     * @param items The item(s) {@code externalId / id} to retrieve.
     * @return The retrieved Transformation.Schedule.
     * @throws Exception
     */
    public List<Transformation.Schedule> retrieve(List<Item> items) throws Exception {
        return retrieveJson(ResourceType.TRANSFORMATIONS_SCHEDULES, items).stream()
                .map(this::parseTransformationSchedules)
                .collect(Collectors.toList());
    }

    /*
 Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
 deal very well with exceptions.
  */
    private Map<String, Object> toRequestInsertItem(Transformation.Schedule element) {
        try {
            return TransformationSchedulesParser.toRequestInsertItem(element);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> toRequestUpdateItem(Transformation.Schedule element) {
        try {
            return TransformationSchedulesParser.toRequestUpdateItem(element);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> toRequestReplaceItem(Transformation.Schedule item) {
        try {
            return TransformationSchedulesParser.toRequestReplaceItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Transformation.Schedule parseTransformationSchedules(String json) {
        try {
            return TransformationSchedulesParser.parseTransformationSchedules(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of an transformation. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getTransformationScheduleId(Transformation.Schedule item) {
        if (item.hasExternalId()) {
            return Optional.of(item.getExternalId());
        } else if (item.hasId()) {
            return Optional.of(String.valueOf(item.getId()));
        } else {
            return Optional.<String>empty();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<TransformationSchedules.Builder> {
        abstract TransformationSchedules build();
    }

}
