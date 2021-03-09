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
import com.cognite.client.dto.SecurityCategory;
import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.ResponseItems;
import com.cognite.client.servicesV1.parser.SecurityCategoryParser;
import com.cognite.client.util.Partition;
import com.google.auto.value.AutoValue;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represents the Cognite security categories api endpoint.
 *
 * It provide methods for reading and writing {@link SecurityCategory}
 */
@AutoValue
public abstract class SecurityCategories extends ApiBase {

    private static Builder builder() {
        return new AutoValue_SecurityCategories.Builder();
    }

    /**
     * Construct a new {@link SecurityCategories} object using the provided configuration-
     *
     * This method is intended for internal use--SDK client should always use {@link CogniteClient} as the entry point
     * to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return The labels api object.
     */
    public static SecurityCategories of(CogniteClient client) {
        return SecurityCategories.builder()
                .setClient(client)
                .build();
    }

    /**
     * Return all {@link SecurityCategory} object that matches the filters set in the {@link SecurityCategory}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into yout own data structure.
     *
     * The security categories are retrieved using multiple, parallel request streams towards the Cognite api. The
     * number of parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     * @param requestParameters
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<SecurityCategory>> list(Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[partitions.size()]));
    }

    /**
     * Returns all {@link SecurityCategory} objects that matches the filters set in the {@link Request} for
     * the specified partitions. This method is intended for advanced use cases you need direct control over the
     * individual partitions. For example, when using the SDK in a distributed environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param requestParameters The filters to use for retrieving the timeseries.
     * @param partitions The partitions to include
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<SecurityCategory>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.SECURITY_CATEGORY, requestParameters, partitions), this::parseSecurityCategories);
    }

    /**
     * Creates a set of {@link SecurityCategory} objects.
     *
     * @param securityCategories The security categories to upsert
     * @return The upserted security categories
     * @throws Exception
     */
    public List<SecurityCategory> create(List<SecurityCategory> securityCategories) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeSecurityCategories();

        UpsertItems<SecurityCategory> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildAuthConfig())
                .withIdFunction(this::getSecurityCategoryName);

        return upsertItems.create(securityCategories).stream()
                .map(this::parseSecurityCategories)
                .collect(Collectors.toList());
    }

    /**
     * Deletes a set of Security Categories.
     *
     * The security categories to delete are identified via their {@code id} by submitting a list of
     * {@link SecurityCategory}.
     *
     * @param securityCategories A list of {@link SecurityCategory} representing the security categories (id) to be deleted
     * @return The deleted security categories via {@link Item}
     * @throws Exception
     */
    public List<SecurityCategory> delete(List<SecurityCategory> securityCategories) throws Exception {
        String loggingPrefix = "delete() - ";
        Instant startInstant = Instant.now();
        LOG.info(loggingPrefix + "Received {} security categories to delete.",
                securityCategories.size());

        List<List<SecurityCategory>> batches = Partition.ofSize(securityCategories, 100);

        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteSecurityCategories();

        for (List<SecurityCategory> batch : batches) {
            List<Long> items = new ArrayList<>();
            for (SecurityCategory item : batch) {
                if (!item.hasId()) {
                    throw new Exception(String.format(loggingPrefix + "Security category must contain an id: %s",
                            item));
                }
                items.add(item.getId().getValue());
            }
            Request request = addAuthInfo(Request.create()
                    .withRootParameter("items", items));
            ResponseItems<String> response = deleteItemWriter.writeItems(request);
            if (!response.isSuccessful()) {
                throw new Exception(String.format(loggingPrefix + "Delete security categories request failed: %s",
                        response.getResponseBodyAsString()));
            }
        }

        LOG.info(loggingPrefix + "Successfully deleted {} security categories. Duration: {}",
                securityCategories.size(),
                Duration.between(startInstant, Instant.now()));

        return securityCategories;
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exception.
     */
    private SecurityCategory parseSecurityCategories(String json) {
        try {
            return SecurityCategoryParser.parseSecurityCategory(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exception.
     */
    private Map<String, Object> toRequestInsertItem(SecurityCategory item) {
        try {
            return SecurityCategoryParser.toRequestInsertItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exception.
     */
    private Optional<String> getSecurityCategoryName(SecurityCategory item) {
        try {
            return Optional.of(item.getName());
        } catch (Exception e) {
            return Optional.<String>empty();
        }
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract SecurityCategories build();
    }
}
