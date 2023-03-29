/*
 * Copyright (c) 2023 Cognite AS
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

package com.cognite.client.datamodel;

import com.cognite.client.ApiBase;
import com.cognite.client.CogniteClient;
import com.cognite.client.Request;
import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.datamodel.DataModel;
import com.cognite.client.dto.datamodel.Space;
import com.cognite.client.dto.datamodel.SpaceReference;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.datamodel.SpacesParser;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This class represents the Cognite data model spaces api endpoint
 *
 * It provides methods for reading and writing {@link Space}
 */
@AutoValue
public abstract class GraphQL extends ApiBase {

    protected static final Logger LOG = LoggerFactory.getLogger(GraphQL.class);

    private static Builder builder() {
        return new AutoValue_GraphQL.Builder();
    }

    /**
     * Construct a new {@link GraphQL} object using the provided configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return The datasets api object.
     */
    public static GraphQL of(CogniteClient client, DataModel dataModel) {
        return GraphQL.builder()
                .setClient(client)
                .setDataModel(dataModel)
                .build();
    }

    public abstract DataModel getDataModel();

    /**
     * Returns all {@link Space} objects.
     *
     * @see #list(Request)
     * @see CogniteClient
     * @see CogniteClient#datasets()
     */
    public Iterator<List<Space>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Return all {@link Space} objects.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer entire results set, then you have to
     * stream these results into your own data structure.
     *
     * @param requestParameters The filters to use for retrieving datasets.
     * @return An {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Space>> list(Request requestParameters) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.SPACE, requestParameters), this::parseSpace);
    }



    /*
    Returns the id of a space.
     */
    private Optional<String> getSpaceId(Space item) {
        return Optional.of(item.getSpace());
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't deal very well
    with exceptions.
     */
    private Space parseSpace(String json) {
        try {
            return SpacesParser.parseSpace(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't deal very well
    with exceptions.
     */
    private Map<String, Object> toRequestUpsertItem(Space item) {
        try {
            return SpacesParser.toRequestUpsertItem(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Parse a space reference to a delete item.
     */
    private Map<String, Object> toDeleteItem(SpaceReference item) {
        return Map.of("space", item.getSpace());
    }

    /*
    Get the id from a SpaceReference
     */
    private Optional<String> getId(SpaceReference item) {
        return Optional.of(item.getSpace());
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<Builder> {
        abstract Builder setDataModel(DataModel value);
        abstract GraphQL build();
    }
}
