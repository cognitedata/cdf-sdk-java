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

import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Aggregate;
import com.cognite.client.dto.Asset;
import com.cognite.client.config.ResourceType;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.Item;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.cognite.client.servicesV1.parser.AssetParser;
import com.cognite.client.util.Partition;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomStringUtils;
import org.jgrapht.Graph;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.alg.cycle.CycleDetector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class represents the Cognite assets api endpoint.
 *
 * It provides methods for reading and writing {@link com.cognite.client.dto.Asset}.
 */
@AutoValue
public abstract class Assets extends ApiBase {
    private final static int MAX_UPSERT_BATCH_SIZE = 200;

    private static Builder builder() {
        return new AutoValue_Assets.Builder();
    }

    protected static final Logger LOG = LoggerFactory.getLogger(Assets.class);

    /**
     * Constructs a new {@link Assets} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static Assets of(CogniteClient client) {
        return Assets.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns all {@link Asset} objects.
     *
     * @see #list(Request)
     */
    public Iterator<List<Asset>> list() throws Exception {
        return this.list(Request.create());
    }

    /**
     * Returns all {@link Asset} objects that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * The assets are retrieved using multiple, parallel request streams towards the Cognite api. The number of
     * parallel streams are set in the {@link com.cognite.client.config.ClientConfig}.
     *
     * @param requestParameters the filters to use for retrieving the assets.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Asset>> list(Request requestParameters) throws Exception {
        List<String> partitions = buildPartitionsList(getClient().getClientConfig().getNoListPartitions());

        return this.list(requestParameters, partitions.toArray(new String[0]));
    }

    /**
     * Returns all {@link Asset} objects that matches the filters set in the {@link Request} for the
     * specified partitions. This is method is intended for advanced use cases where you need direct control over
     * the individual partitions. For example, when using the SDK in a distributed computing environment.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param requestParameters the filters to use for retrieving the assets.
     * @param partitions the partitions to include.
     * @return an {@link Iterator} to page through the results set.
     * @throws Exception
     */
    public Iterator<List<Asset>> list(Request requestParameters, String... partitions) throws Exception {
        return AdapterIterator.of(listJson(ResourceType.ASSET, requestParameters, partitions), this::parseAsset);
    }

    /**
     * Retrieve assets by id.
     *
     * @param items The item(s) {@code externalId / id} to retrieve.
     * @return The retrieved events.
     * @throws Exception
     */
    public List<Asset> retrieve(List<Item> items) throws Exception {
        return retrieveJson(ResourceType.ASSET, items).stream()
                .map(this::parseAsset)
                .collect(Collectors.toList());
    }

    /**
     * Performs an item aggregation request to Cognite Data Fusion.
     *
     * The default aggregation is a total item count based on the (optional) filters in the request.
     * Multiple aggregation types are supported. Please refer to the Cognite API specification for more information
     * on the possible settings.
     *
     * @param requestParameters The filtering and aggregates specification
     * @return The aggregation results.
     * @throws Exception
     * @see <a href="https://docs.cognite.com/api/v1/">Cognite API v1 specification</a>
     */
    public Aggregate aggregate(Request requestParameters) throws Exception {
        return aggregate(ResourceType.ASSET, requestParameters);
    }

    /**
     * Synchronizes the input collection of {@link Asset} (representing multiple, complete asset hierarchies)
     * with existing asset hierarchies in CDF.
     *
     * This method will inspect the input collection of {@link Asset} and identify the various asset hierarchies. Each
     * hierarchy is then processed by {@link #synchronizeHierarchy(Collection)}.
     *
     * @see #synchronizeHierarchy(Collection)
     *
     * @param assetHierarchies The input asset hierarchies--this represents the target state of the synchronization.
     * @return the synchronized assets.
     * @throws Exception
     */
    public List<Asset> synchronizeMultipleHierarchies(Collection<Asset> assetHierarchies) throws Exception {
        String loggingPrefix = "synchronizeMultipleHierarchies() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Instant startInstant = Instant.now();

        if (!checkExternalId(assetHierarchies)) {
            String message = loggingPrefix + "Some input assets are missing externalId.";
            LOG.error(message);
            throw new Exception(message);
        }

        Map<String, Asset> assetMap = new HashMap<>();
        assetHierarchies
                .forEach(asset -> assetMap.put(asset.getExternalId(), asset));

        // Identify the hierarchies by loading all assets into a graph and identify the connected sub-graphs
        Graph<Asset, DefaultEdge> graph = new SimpleDirectedGraph<>(DefaultEdge.class);

        // add vertices
        for (Asset vertex : assetMap.values()) {
            graph.addVertex(vertex);
        }
        // add edges
        for (Asset asset : assetMap.values()) {
            if (asset.hasParentExternalId() && assetMap.containsKey(asset.getParentExternalId())) {
                graph.addEdge(assetMap.get(asset.getParentExternalId()), asset);
            }
        }
        ConnectivityInspector<Asset, DefaultEdge> connectivityInspector = new ConnectivityInspector<>(graph);
        List<Set<Asset>> hierarchies = connectivityInspector.connectedSets();
        LOG.info(loggingPrefix + "Identified {} hierarchies in the input collection. Duration: {}",
                hierarchies.size(),
                Duration.between(startInstant, Instant.now()));

        List<Asset> returnList = new ArrayList<>();
        for (Set<Asset> hierarchy : hierarchies) {
            returnList.addAll(this.synchronizeHierarchy(hierarchy));
        }
        LOG.info(loggingPrefix + "Finished synchronizing {} hierarchies. Duration: {}",
                hierarchies.size(),
                Duration.between(startInstant, Instant.now()));

        return returnList;
    }

    /**
     * Synchronizes the input collection of {@link Asset} (representing a single, complete asset hierarchy)
     * with an existing asset hierarchy in CDF. The input asset collection represents the target state.
     * New asset nodes will be added, changed asset nodes will be updated
     * and deleted asset nodes will be removed (from CDF).
     *
     * Algorithm:
     * - Verify that the input collection satisfies the hierarchy constraints:
     *      - All assets must specify an {@code externalId}.
     *      - No duplicates (based on {@code externalId}).
     *      - The collection must contain one and only one asset object with no parent reference (representing the root node)
     *      - All other assets must contain a valid {@code parentExternalId} reference (no self-references).
     *      - No circular references.
     * - Read the CDF asset hierarchy based on the supplied root external id.
     * - Compare the input collection with the existing CDF hierarchy. Identify creates, updates and deletes.
     * - Write creates and updates in topological order.
     * - Write deletes in reverse topological order.
     *
     * @param assetHierarchy The input asset hierarchy--this represents the target state of the synchronization.
     * @return the synchronized assets.
     * @throws Exception
     */
    public List<Asset> synchronizeHierarchy(Collection<Asset> assetHierarchy) throws Exception {
        String loggingPrefix = "synchronizeHierarchy() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Instant startInstant = Instant.now();

        LOG.info(loggingPrefix + "Start hierarchy synchronization. Received {} assets in the input collection",
                assetHierarchy.size());
        LOG.debug(loggingPrefix + "Check input collection data integrity.");
        if (!verifyAssetHierarchyIntegrity(assetHierarchy)) {
            String message = loggingPrefix + "The input asset collection does not satisfy the integrity constraints.";
            LOG.error(message);
            throw new Exception(message);
        }
        LOG.debug(loggingPrefix + "Input collection data integrity is validated. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        LOG.debug(loggingPrefix + "Identify root node.");
        List<Asset> rootNodes = identifyRootNodes(assetHierarchy);
        if (rootNodes.size() != 1) {
            String message = String.format("%s Error, found %d root nodes. Expected to find one.",
                    loggingPrefix,
                    rootNodes.size());
            LOG.error(message);
            throw new Exception(message);
        }
        String rootExternalId = rootNodes.get(0).getExternalId();
        LOG.info(loggingPrefix + "Identified root node with external id [{}]. Duration: {}",
                rootExternalId,
                Duration.between(startInstant, Instant.now()));

        LOG.debug(loggingPrefix + "Start downloading the existing asset hierarchy for root [{}]",
                rootExternalId);
        // Use the subtree query. Must check the constraints (max 100k)
        Map<String, Asset> cdfAssets = new HashMap<>();
        Request assetRequest = Request.create()
                .withFilterParameter("assetSubtreeIds", List.of(Map.of("externalId", rootExternalId)));
        getClient().assets().list(assetRequest)
                .forEachRemaining(assetBatch -> {
                    Map<String, Asset> batchMap = assetBatch.stream()
                            .collect(Collectors.toMap(Asset::getExternalId, Function.identity()));
                    cdfAssets.putAll(batchMap);
                });

        LOG.debug(loggingPrefix + "Finished downloading the existing asset hierarchy for root [{}]. "
                + "No assets: {}. Duration {}",
                rootExternalId,
                cdfAssets.size(),
                Duration.between(startInstant, Instant.now()));

        // Identify upserts and deletes
        List<Asset> upsertList = new ArrayList<>();
        List<Asset> noChangeList = new ArrayList<>();
        List<Item> deleteList = new ArrayList<>();
        int changedAssetCounter = 0;
        int noChangeCounter = 0;
        int newAssetCounter = 0;
        for (Asset inputAsset : assetHierarchy) {
            if (cdfAssets.containsKey(inputAsset.getExternalId())) {
                // The asset exists from before. Check if it has changed.
                if (!isEqual(inputAsset, cdfAssets.get(inputAsset.getExternalId()))) {
                    upsertList.add(inputAsset);
                    changedAssetCounter++;
                } else {
                    noChangeList.add(cdfAssets.get(inputAsset.getExternalId())); // Add from CDF to get id fields++
                    noChangeCounter++;
                }
                cdfAssets.remove(inputAsset.getExternalId());
            } else {
                // A new asset, add it to the upserts list
                upsertList.add(inputAsset);
                newAssetCounter++;
            }
        }

        // Check for leftover CDF assets. These should be deleted
        if (!cdfAssets.isEmpty()) {
            List<Asset> sortedAssets = topologicalSort(cdfAssets.values());
            Collections.reverse(sortedAssets); // Should delete assets in reverse topological order
            deleteList = sortedAssets.stream()
                    .map(asset ->
                        Item.newBuilder()
                            .setExternalId(asset.getExternalId())
                            .build())
                    .collect(Collectors.toList());
        }

        LOG.info(loggingPrefix + "Change detection complete. New assets: {}, changed assets: {}, deleted assets: {}, "
                        + "assets with no change: {}. Duration {}",
                newAssetCounter,
                changedAssetCounter,
                deleteList.size(),
                noChangeCounter,
                Duration.between(startInstant, Instant.now()));

        // Write the changes to CDF
        // The upserts must be written with REPLACE mode
        List<Asset> upsertedAssets = getClient()
                .withClientConfig(getClient().getClientConfig()
                        .withUpsertMode(UpsertMode.REPLACE))
                .assets()
                .upsert(upsertList);

        getClient().assets().delete(deleteList);
        LOG.info(loggingPrefix + "Finished synchronizing hierarchy. Root asset external id: [{}]. "
                + "Total no assets: {}. Upserted assets: {}. Deleted assets: {}. Duration: {}",
                rootExternalId,
                assetHierarchy.size(),
                upsertList.size(),
                deleteList.size(),
                Duration.between(startInstant, Instant.now()));

        return Stream.concat(noChangeList.stream(), upsertedAssets.stream())
                .collect(Collectors.toList());
    }

    /**
     * Creates or updates a set of {@link Asset} objects.
     *
     * If it is a new {@link Asset} object (based on {@code id / externalId}, then it will be created.
     *
     * If an {@link Asset} object already exists in Cognite Data Fusion, it will be updated. The update behavior
     * is specified via the update mode in the {@link com.cognite.client.config.ClientConfig} settings.
     *
     * The assets will be checked for integrity and topologically sorted before an ordered upsert operation
     * is started.
     *
     * The following constraints will be evaluated:
     * - All assets must specify an {@code externalId}.
     * - No duplicates (based on {@code externalId}.
     * - No self-reference.
     * - No circular references.
     *
     * @param assets The assets to upsert.
     * @return The upserted assets.
     * @throws Exception
     */
    public List<Asset> upsert(Collection<Asset> assets) throws Exception {
        String loggingPrefix = "upsert() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Instant startInstant = Instant.now();
        // Check integrity and sort assets topologically
        List<Asset> sortedAssets = topologicalSort(assets);

        // Setup writers and upsert manager
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter createItemWriter = connector.writeAssets();
        ConnectorServiceV1.ItemWriter updateItemWriter = connector.updateAssets();

        UpsertItems<Asset> upsertItems = UpsertItems.of(createItemWriter, this::toRequestInsertItem, getClient().buildAuthConfig())
                .withUpdateItemWriter(updateItemWriter)
                .withUpdateMappingFunction(this::toRequestUpdateItem)
                .withIdFunction(this::getAssetId);

        if (getClient().getClientConfig().getUpsertMode() == UpsertMode.REPLACE) {
            upsertItems = upsertItems.withUpdateMappingFunction(this::toRequestReplaceItem);
        }

        // Write assets as a serial set of single-worker requests. Must do this to guarantee the order of upsert.
        List<List<Asset>> sortedBatches = Partition.ofSize(sortedAssets, MAX_UPSERT_BATCH_SIZE);
        List<String> assetUpsertResults = new ArrayList<>(assets.size());
        for (List<Asset> batch : sortedBatches) {
            assetUpsertResults.addAll(upsertItems.upsertViaCreateAndUpdate(batch));
        }

        LOG.info(loggingPrefix + "Finished upserting {} assets across {} batches within a duration of {}",
                sortedAssets.size(),
                sortedBatches.size(),
                Duration.between(startInstant, Instant.now()));
        return assetUpsertResults.stream()
                .map(this::parseAsset)
                .collect(Collectors.toList());
    }

    /**
     * Checks a collection of assets for integrity. The assets must represent a single, complete
     * hierarchy.
     *
     * This verifies if the collection satisfies the
     * constraints of the Cognite Data Fusion data model if you were to write them using the
     * {@link #upsert(Collection) upsert} method.
     *
     * The following constraints will be evaluated:
     * - All assets must specify an {@code externalId}.
     * - No duplicates (based on {@code externalId}.
     * - The collection must contain one and only one asset object with no parent reference (representing the root node)
     * - All other assets must contain a valid {@code parentExternalId} reference (no self-references).
     * - No circular references.
     *
     * @param assets A collection of {@link Asset} representing a single, complete asset hierarchy.
     * @return
     */
    public boolean verifyAssetHierarchyIntegrity(Collection<Asset> assets) {
        if (!checkExternalId(assets)) return false;
        if (!checkDuplicates(assets)) return false;
        if (!checkSelfReference(assets)) return false;
        if (!checkCircularReferences(assets)) return false;
        if (!checkReferentialIntegrity(assets)) return false;

        return true;
    }

    /**
     * Deletes a set of assets.
     *
     * The assets to delete are identified via their {@code externalId / id} by submitting a list of
     * {@link Item}.
     *
     * This method will not delete assets recursively. Please use {@code delete(List<Item> items, boolean recursive)}
     * for recursive deletes.
     *
     * @param items a list of {@link Item} representing the assets (externalId / id) to be deleted
     * @return The deleted events via {@link Item}
     * @throws Exception
     */
    public List<Item> delete(List<Item> items) throws Exception {
        return delete(items, false);
    }

    /**
     * Deletes a set of assets.
     *
     * The assets to delete are identified via their {@code externalId / id} by submitting a list of
     * {@link Item}.
     *
     * @param items a list of {@link Item} representing the assets (externalId / id) to be deleted
     * @param recursive Set to {@code true} to recursively delete all subtrees under the specified items.
     * @return The deleted events via {@link Item}
     * @throws Exception
     */
    public List<Item> delete(List<Item> items, boolean recursive) throws Exception {
        ConnectorServiceV1 connector = getClient().getConnectorService();
        ConnectorServiceV1.ItemWriter deleteItemWriter = connector.deleteAssets();

        DeleteItems deleteItems = DeleteItems.of(deleteItemWriter, getClient().buildAuthConfig())
                .addParameter("ignoreUnknownIds", true)
                .addParameter("recursive", recursive);

        return deleteItems.deleteItems(items);
    }

    /**
     * Find and return the root nodes (if any) in the assets collection. A root node is an asset that doesn't
     * specify a parent external id reference.
     *
     * @param assets The assets to search for root nodes.
     * @return a list of asset root nodes.
     */
    private List<Asset> identifyRootNodes(Collection<Asset> assets) {
        String loggingPrefix = "identifyRootNodes() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        LOG.debug(loggingPrefix + "Start identifying root nodes.");
        List<Asset> rootNodeList = assets.stream()
                .filter(asset -> asset.getParentExternalId().isBlank())
                .collect(Collectors.toList());

        LOG.debug(loggingPrefix + "Found {} root nodes.",
                rootNodeList.size());

        return rootNodeList;
    }

    /**
     * Check if two asset objects are equal in terms of their main payload. Internal attributes like ids and
     * internal timestamps are ignored.
     *
     * @param one The first asset to compare.
     * @param other The second asset to compare.
     * @return true if the assets carry an equal payload, false if not.
     */
    private boolean isEqual(Asset one, Asset other) {
        boolean result = true;

        result = result && (one.hasExternalId() == other.hasExternalId());
        if (one.hasExternalId()) {
            result = result && one.getExternalId()
                    .equals(other.getExternalId());
        }

        result = result && one.getName()
                .equals(other.getName());

        result = result && (one.hasParentExternalId() == other.hasParentExternalId());
        if (one.hasParentExternalId()) {
            result = result && one.getParentExternalId()
                    .equals(other.getParentExternalId());
        }

        result = result && (one.hasDescription() == other.hasDescription());
        if (one.hasDescription()) {
            result = result && one.getDescription()
                    .equals(other.getDescription());
        }

        result = result && one.getMetadataMap().equals(
                other.getMetadataMap());

        result = result && (one.hasSource() == other.hasSource());
        if (one.hasSource()) {
            result = result && one.getSource()
                    .equals(other.getSource());
        }

        result = result && (one.hasDataSetId() == other.hasDataSetId());
        if (one.hasDataSetId()) {
            result = result && one.getDataSetId() == other.getDataSetId();
        }

        result = result && (one.getLabelsList().equals(other.getLabelsList()));

        return result;
    }

    /**
     * This function will sort a collection of assets into the correct order for upsert to CDF.
     *
     * Assets need to be written in a certain order to comply with the hierarchy constraints of CDF. In short, if an asset
     * references a parent (either via id or externalId), then that parent must exist either in CDF or in the same write
     * batch. Hence, a collection of assets must be written to CDF in topological order.
     *
     * This function requires that the input collection is complete. That is, all assets required to traverse the hierarchy
     * from the root node to the leaves must either exist in CDF and/or in the input collection.
     *
     * The sorting algorithm employed is a naive breadth-first O(n * depth(n)):
     * while (items in inputCollection) {
     *     for (items in inputCollection) {
     *         if (item references unknown OR item references null OR item references id) : write item and remove from inputCollection
     *     }
     * }
     *
     * Other requirements for the input:
     * - Assets must have externalId set.
     * - No duplicates.
     * - No self-references.
     * - No circular references.
     * - If both parentExternalId and parentId are set, then parentExternalId takes precedence in the sort.
     *
     * @param assets A collection of {@link Asset} to be topologically sorted.
     * @return The sorted assets collection.
     * @throws Exception if one (or more) of the constraints are not fulfilled.
     */
    private List<Asset> topologicalSort(Collection<Asset> assets) throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "sortAssetsForUpsert - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Preconditions.checkArgument(checkExternalId(assets),
                "Some assets do not have externalId. Please check the log for more information.");
        Preconditions.checkArgument(checkDuplicates(assets),
                "Found duplicates in the input assets. Please check the log for more information.");
        Preconditions.checkArgument(checkSelfReference(assets),
                "Some assets contain a self-reference. Please check the log for more information.");
        Preconditions.checkArgument(checkCircularReferences(assets),
                "Circular reference detected. Please check the log for more information.");

        LOG.debug(loggingPrefix + "Constraints check passed. Starting sort.");
        Map<String, Asset> inputMap = assets.stream()
                .collect(Collectors.toMap(asset -> asset.getExternalId(), Function.identity()));
        List<Asset> sortedAssets = new ArrayList<>();


        while (!inputMap.isEmpty()) {
            int startInputMapSize = inputMap.size();
            LOG.debug(loggingPrefix + "Starting new sort iteration. Assets left to sort: {}", inputMap.size());
            for (Iterator<Asset> iterator = inputMap.values().iterator(); iterator.hasNext();) {
                Asset asset = iterator.next();
                if (asset.hasParentExternalId()) {
                    // Check if the parent asset exists in the input collection. If no, it is safe to write the asset.
                    if (!inputMap.containsKey(asset.getParentExternalId())) {
                        sortedAssets.add(asset);
                        iterator.remove();
                    }
                } else {
                    // Asset either has no parent reference or references an (internal) id.
                    // Null parent is safe to write and (internal) id is assumed to already exist in cdf--safe to write.
                    sortedAssets.add(asset);
                    iterator.remove();
                }
            }
            LOG.debug(loggingPrefix + "Finished sort iteration. Assets left to sort: {}", inputMap.size());
            if (startInputMapSize == inputMap.size()) {
                String message = loggingPrefix + "Possible circular reference detected when sorting assets, aborting.";
                LOG.error(message);
                throw new Exception(message);
            }
        }
        LOG.info(loggingPrefix + "Sort assets finished. Sorted {} assets within a duration of {}.",
                sortedAssets.size(),
                Duration.between(startInstant, Instant.now()).toString());

        return sortedAssets;
    }

    /**
     * Checks the assets for {@code externalId}.
     *
     * @param assets The assets to check.
     * @return true if all assets contain {@code externalId}. False if one or more assets do not have {@code externalId}.
     */
    private boolean checkExternalId(Collection<Asset> assets) {
        String loggingPrefix = "checkExternalId() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        List<Asset> missingExternalIdList = new ArrayList<>();

        for (Asset asset : assets) {
            if (!asset.hasExternalId()) {
                missingExternalIdList.add(asset);
            }
        }

        // Report on missing externalId
        if (missingExternalIdList.size() > 0) {
            StringBuilder message = new StringBuilder();
            String errorMessage = loggingPrefix + "Found " + missingExternalIdList.size() + " assets missing externalId.";
            message.append(errorMessage).append(System.lineSeparator());
            if (missingExternalIdList.size() > 100) {
                missingExternalIdList = missingExternalIdList.subList(0, 99);
            }
            message.append("Items with missing externalId (max 100 displayed): " + System.lineSeparator());
            for (Asset item : missingExternalIdList) {
                message.append("---------------------------").append(System.lineSeparator())
                        .append("name: [").append(item.getName()).append("]").append(System.lineSeparator())
                        .append("parentExternalId: [").append(item.getParentExternalId()).append("]").append(System.lineSeparator())
                        .append("description: [").append(item.getDescription()).append("]").append(System.lineSeparator())
                        .append("--------------------------");
            }
            LOG.warn(message.toString());
            return false;
        }
        LOG.debug(loggingPrefix + "All assets contain an externalId.");

        return true;
    }

    /**
     * Checks the assets for duplicates. The duplicates check is based on {@code externalId}.
     *
     * @param assets The assets to check.
     * @return true if no duplicates are detected. False if one or more duplicates are detected.
     */
    private boolean checkDuplicates(Collection<Asset> assets) {
        String loggingPrefix = "checkDuplicates() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        List<Asset> duplicatesList = new ArrayList<>();
        Map<String, Asset> inputMap = new HashMap<>();

        for (Asset asset : assets) {
            if (inputMap.containsKey(asset.getExternalId())) {
                duplicatesList.add(asset);
            }
            inputMap.put(asset.getExternalId(), asset);
        }

        // Report on duplicates
        if (duplicatesList.size() > 0) {
            StringBuilder message = new StringBuilder();
            String errorMessage = loggingPrefix + "Found " + duplicatesList.size() + " duplicates.";
            message.append(errorMessage).append(System.lineSeparator());
            if (duplicatesList.size() > 100) {
                duplicatesList = duplicatesList.subList(0, 99);
            }
            message.append("Duplicate items (max 100 displayed): " + System.lineSeparator());
            for (Asset item : duplicatesList) {
                message.append("---------------------------").append(System.lineSeparator())
                        .append("externalId: [").append(item.getExternalId()).append("]").append(System.lineSeparator())
                        .append("name: [").append(item.getName()).append("]").append(System.lineSeparator())
                        .append("description: [").append(item.getDescription()).append("]").append(System.lineSeparator())
                        .append("--------------------------");
            }
            LOG.warn(message.toString());
            return false;
        }

        LOG.debug(loggingPrefix + "No duplicates detected in the assets collection.");
        return true;
    }

    /**
     * Checks the assets for self-reference. That is, the asset's {@code parentExternalId} references
     * its own {@code externalId}.
     *
     * @param assets The assets to check.
     * @return true if no self-references are detected. False if one or more self-reference are detected.
     */
    private boolean checkSelfReference(Collection<Asset> assets) {
        String loggingPrefix = "checkExternalId() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        List<Asset> selfReferenceList = new ArrayList<>(50);

        for (Asset asset : assets) {
            if (asset.hasParentExternalId()
                    && asset.getParentExternalId().equals(asset.getExternalId())) {
                selfReferenceList.add(asset);
            }
        }

        // Report on self-references
        if (!selfReferenceList.isEmpty()) {
            StringBuilder message = new StringBuilder();
            String errorMessage = loggingPrefix + "Found " + selfReferenceList.size()
                    + " items with self-referencing parentExternalId.";
            message.append(errorMessage).append(System.lineSeparator());
            if (selfReferenceList.size() > 100) {
                selfReferenceList = selfReferenceList.subList(0, 99);
            }
            message.append("Items with self-reference (max 100 displayed): " + System.lineSeparator());
            for (Asset item : selfReferenceList) {
                message.append("---------------------------").append(System.lineSeparator())
                        .append("externalId: [").append(item.getExternalId()).append("]").append(System.lineSeparator())
                        .append("name: [").append(item.getName()).append("]").append(System.lineSeparator())
                        .append("description: [").append(item.getDescription()).append("]").append(System.lineSeparator())
                        .append("parentExternalId: [").append(item.getParentExternalId()).append("]").append(System.lineSeparator())
                        .append("--------------------------");
            }
            LOG.warn(message.toString());
            return false;
        }
        LOG.debug(loggingPrefix + "No self-references detected in the assets collection.");

        return true;
    }

    /**
     * Checks the assets for circular references.
     *
     * @param assets The assets to check.
     * @return True if circular references are detected. False if no circular references are detected.
     */
    private boolean checkCircularReferences(Collection<Asset> assets) {
        String loggingPrefix = "checkCircularReferences() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Map<String, Asset> assetMap = new HashMap<>();
        assets.forEach(asset -> assetMap.put(asset.getExternalId(), asset));

        // Checking for circular references
        Graph<Asset, DefaultEdge> graph = new SimpleDirectedGraph<>(DefaultEdge.class);

        // add vertices
        for (Asset vertex : assetMap.values()) {
            graph.addVertex(vertex);
        }
        // add edges
        for (Asset asset : assetMap.values()) {
            if (asset.hasParentExternalId() && assetMap.containsKey(asset.getParentExternalId())) {
                graph.addEdge(assetMap.get(asset.getParentExternalId()), asset);
            }
        }

        CycleDetector<Asset, DefaultEdge> cycleDetector = new CycleDetector<>(graph);
        if (cycleDetector.detectCycles()) {
            List<String> cycles = cycleDetector.findCycles().stream()
                    .map(Asset::getExternalId)
                    .collect(Collectors.toList());

            String message = loggingPrefix + "Cycles detected. Number of asset in the cycle: " + cycles.size();
            LOG.error(message);
            LOG.error(loggingPrefix + "Cycles: " + cycles.toString());
            return false;
        }

        LOG.debug(loggingPrefix + "No cycles detected in the assets collection.");
        return true;
    }

    /**
     * Checks the assets for referential integrity.
     *
     * This check is relevant for a collection of assets that represents a single, complete
     * asset hierarchy. It performs the following checks:
     * - One and only one root node (an asset with no parent reference).
     * - All other assets reference an asset in this collection.
     *
     * @param assets The assets to check.
     * @return True if integrity is intact. False otherwise.
     */
    private boolean checkReferentialIntegrity(Collection<Asset> assets) {
        String loggingPrefix = "checkReferentialIntegrity() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Set<String> extIdSet = new HashSet<>();
        List<Asset> invalidReferenceList = new ArrayList<>();
        List<Asset> rootNodeList = new ArrayList<>();

        LOG.debug(loggingPrefix + "Checking asset input table for integrity.");
        for (Asset element : assets) {
            if (!element.hasParentExternalId()) {
                rootNodeList.add(element);
            }
            extIdSet.add(element.getExternalId());
        }

        for (Asset element : assets) {
            if (element.hasParentExternalId()
                    && !extIdSet.contains(element.getParentExternalId())) {
                invalidReferenceList.add(element);
            }
        }

        if (rootNodeList.size() != 1) {
            String errorMessage = loggingPrefix + "Found " + rootNodeList.size() + " root nodes.";
            StringBuilder message = new StringBuilder()
                    .append(errorMessage).append(System.lineSeparator());
            if (rootNodeList.size() > 100) {
                rootNodeList = rootNodeList.subList(0, 99);
            }
            message.append("Root nodes (max 100 displayed): " + System.lineSeparator());
            for (Asset item : rootNodeList) {
                message.append("---------------------------").append(System.lineSeparator())
                        .append("externalId: [").append(item.getExternalId()).append("]").append(System.lineSeparator())
                        .append("name: [").append(item.getName()).append("]").append(System.lineSeparator())
                        .append("parentExternalId: [").append(item.getParentExternalId()).append("]").append(System.lineSeparator())
                        .append("description: [").append(item.getDescription()).append("]").append(System.lineSeparator())
                        .append("--------------------------");
            }
            LOG.warn(message.toString());
            return false;
        }

        if (invalidReferenceList.size() > 0) {
            StringBuilder message = new StringBuilder();
            String errorMessage = loggingPrefix + "Found " + invalidReferenceList.size() + " assets with invalid parent reference.";
            message.append(errorMessage).append(System.lineSeparator());
            if (invalidReferenceList.size() > 100) {
                invalidReferenceList = invalidReferenceList.subList(0, 99);
            }
            message.append("Items with invalid parentExternalId (max 100 displayed): " + System.lineSeparator());
            for (Asset item : invalidReferenceList) {
                message.append("---------------------------").append(System.lineSeparator())
                        .append("externalId: [").append(item.getExternalId()).append("]").append(System.lineSeparator())
                        .append("name: [").append(item.getName()).append("]").append(System.lineSeparator())
                        .append("parentExternalId: [").append(item.getParentExternalId()).append("]").append(System.lineSeparator())
                        .append("description: [").append(item.getDescription()).append("]").append(System.lineSeparator())
                        .append("--------------------------");
            }
            LOG.warn(message.toString());
            return false;
        }

        LOG.debug(loggingPrefix + "The asset collection contains a single root node and valid parent references.");
        return true;
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Asset parseAsset(String json) {
        try {
            return AssetParser.parseAsset(json);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestInsertItem(Asset item) {
        try {
            return AssetParser.toRequestInsertItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestUpdateItem(Asset item) {
        try {
            return AssetParser.toRequestUpdateItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Wrapping the parser because we need to handle the exception--an ugly workaround since lambdas don't
    deal very well with exceptions.
     */
    private Map<String, Object> toRequestReplaceItem(Asset item) {
        try {
            return AssetParser.toRequestReplaceItem(item);
        } catch (Exception e)  {
            throw new RuntimeException(e);
        }
    }

    /*
    Returns the id of an event. It will first check for an externalId, second it will check for id.

    If no id is found, it returns an empty Optional.
     */
    private Optional<String> getAssetId(Asset item) {
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
        abstract Assets build();
    }
}
