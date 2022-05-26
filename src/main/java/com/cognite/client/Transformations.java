package com.cognite.client;

import com.cognite.client.config.ResourceType;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.*;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ItemReader;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.ThreeDModelParser;
import com.cognite.client.servicesV1.parser.ThreeDModelRevisionParser;
import com.cognite.client.servicesV1.parser.TimeseriesParser;
import com.cognite.client.servicesV1.parser.TransformationParser;
import com.google.auto.value.AutoValue;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@AutoValue
public abstract class Transformations extends ApiBase {

    private static Transformations.Builder builder() {
        return new AutoValue_Transformations.Builder();
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
    public static Transformations of(CogniteClient client) {
        return Transformations.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns {@link TransformationJobs} representing TransformationJobs api endpoints.
     *
     * @return The TransformationJobs api endpoints.
     */
    public TransformationJobs jobs() {
        return TransformationJobs.of(getClient());
    }

    /**
     * Returns {@link TransformationSchedules} representing TransformationJobs api endpoints.
     *
     * @return The TransformationJobs api endpoints.
     */
    public TransformationSchedules schedules() {
        return TransformationSchedules.of(getClient());
    }

    /**
     * Returns {@link TransformationNotifications} representing TransformationJobs api endpoints.
     *
     * @return The TransformationNotifications api endpoints.
     */
    public TransformationNotifications notifications() {
        return TransformationNotifications.of(getClient());
    }


    /**
     * Returns all {@link Transformation} objects.
     *
     * @see #list(Request)
     */
    public Iterator<List<Transformation>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Returns all {@link Transformation} object that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The Transformations are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param requestParameters the filters to use for retrieving timeseries.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Transformation>> list(Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link Transformation} objects that matches the filters set in the {@link Request} for
     * the specified partitions. This method is intended for advanced use cases where you need direct control over the
     * individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param requestParameters the filters to use for retrieving the timeseries.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Transformation>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.TRANSFORMATIONS, requestParameters, partitions), this::parseTransformations);
    }

    public List<Transformation> upsert(List<Transformation> transformations) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeTransformation();
        ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateTransformation();

        UpsertItems<Transformation> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildAuthConfig())
                .withUpdateItemWriter(updateItemWriter)
                .withUpdateMappingFunction(this::toRequestUpdateItem)
                .withIdFunction(this::getTransformationId);

        if (getClient().getClientConfig().getUpsertMode() == UpsertMode.REPLACE) {
            upsertItems = upsertItems.withUpdateMappingFunction(this::toRequestReplaceItem);
        }

        return upsertItems.upsertViaCreateAndUpdate(transformations).stream()
                .map(this::parseTransformations)
                .collect(Collectors.toList());
    }

    public List<Transformation> retrieve(List<Item> items) throws Exception {
        return retrieveJson(ResourceType.TRANSFORMATIONS, items).stream()
                .map(this::parseTransformations)
                .collect(Collectors.toList());
    }

    public List<Item> delete(List<Item> deleteItemsInput) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteTransformations();

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildAuthConfig())
                .addParameter("ignoreUnknownIds", true);

        return deleteItems.deleteItems(deleteItemsInput);
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Transformation parseTransformations(String json) {
        try {
            return TransformationParser.parseTransformations(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private List<Transformation> parseTransformationsToList(String json) {
        try {
            return TransformationParser.parseTransformationsToList(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
  Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
  deal very well with exceptions.
   */
    private Map<String, Object> toRequestInsertItem(Transformation element) {
        try {
            return TransformationParser.toRequestInsertItem(element);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> toRequestUpdateItem(Transformation element) {
        try {
            return TransformationParser.toRequestUpdateItem(element);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> toRequestReplaceItem(Transformation item) {
        try {
            return TransformationParser.toRequestReplaceItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of an transformation. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getTransformationId(Transformation item) {
        if (StringUtils.isNotBlank(item.getExternalId())) {
            return Optional.of(item.getExternalId());
        } else if (item.hasId()) {
            return Optional.of(String.valueOf(item.getId()));
        } else {
            return Optional.<String>empty();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Transformations.Builder> {
        abstract Transformations build();
    }

}
