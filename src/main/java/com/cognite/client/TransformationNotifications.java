package com.cognite.client;

import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.Transformation;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.TransformationNotificationsParser;
import com.google.auto.value.AutoValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@AutoValue
public abstract class TransformationNotifications extends ApiBase {

    private static TransformationNotifications.Builder builder() {
        return new AutoValue_TransformationNotifications.Builder();
    }

    /**
     * Constructs a new {@link TransformationNotifications} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static TransformationNotifications of(CogniteClient client) {
        return TransformationNotifications.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link Transformation.Notification} objects.
     *
     * @see #list(Request)
     */
    public Iterator<List<Transformation.Notification>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Returns all {@link Transformation.Notification} object that matches the filters set in the {@link Request}.
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
    public Iterator<List<Transformation.Notification>> list(Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link Transformation.Notification} objects that matches the filters set in the {@link Request} for
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
    public Iterator<List<Transformation.Notification>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.TRANSFORMATIONS_NOTIFICATIONS, requestParameters, partitions), this::parseTransformationNotifications);
    }

    /**
     * Subscribe to receive notifications
     *
     * @param subscribes Contains email and id of transformation
     * @return
     * @throws Exception
     */
    public List<Transformation.Notification> subscribe(List<Transformation.Notification.Subscribe> subscribes) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeTransformationNotifications();

        List<Map<String, Object>> insertItems = new ArrayList<>();
        subscribes.forEach(subscribe -> insertItems.add(toRequestInsertItem(subscribe)));

        Request requestInsert = Request.create().withItems(insertItems);
        CompletableFuture<ResponseItems<String>> responseInsert =
                createItemWriter.writeItemsAsync(addAuthInfo(requestInsert));
        responseInsert.join();

        //Collect the responses from the futures
        List<Transformation.Notification> listResponse = new ArrayList<>();
        String exceptionMessage = "";
        ResponseItems<String> response = responseInsert.get();
        if (response.isSuccessful()) {
            listResponse.addAll(parseTransformationNotificationsToList(response.getResponseBodyAsString()));
        } else {
            exceptionMessage = response.getResponseBodyAsString();
            LOG.debug("Upsert items request failed: {}", exceptionMessage);
        }
        return listResponse;
    }

    /**
     * Delete notifications
     *
     * @param items The item(s) {@code externalId / id} to delete.
     * @return
     * @throws Exception
     */
    public Boolean delete(List<Item> items) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteTransformationNotifications();

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildAuthConfig());

        return deleteItems.deleteItems(items).isEmpty() ? false : true;
    }

    /*
   Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
   deal very well with exceptions.
    */
    private Transformation.Notification parseTransformationNotifications(String json) {
        try {
            return TransformationNotificationsParser.parseTransformationNotifications(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private List<Transformation.Notification> parseTransformationNotificationsToList(String json) {
        try {
            return TransformationNotificationsParser.parseTransformationNotificationsToList(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> toRequestInsertItem(Transformation.Notification.Subscribe element) {
        try {
            return TransformationNotificationsParser.toRequestInsertItem(element);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<TransformationNotifications.Builder> {
        abstract TransformationNotifications build();
    }
}
