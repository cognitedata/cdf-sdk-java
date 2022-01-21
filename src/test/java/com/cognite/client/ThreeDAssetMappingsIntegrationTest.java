package com.cognite.client;

import com.cognite.client.dto.*;
import com.cognite.client.util.DataGenerator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ThreeDAssetMappingsIntegrationTest extends ThreeDBaseTest {

    public static final long PUBLIC_DATA_MODEL_ID = 3356984403684032l;
    public static final long PUBLIC_DATA_REVISION_ID = 6664823881595566l;
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Override
    Logger getLogger() {
        return LOG;
    }

    @Test
    @Tag("remoteCDP")
    void testWriteListAndDeleteThreeAssetMappings() throws Exception {
        Instant startInstant = Instant.now();
        LOG.info("Start writing 3D Asset Mappings.");
        String loggingPrefix = "UnitTest - testWriteThreeAssetMappings - ";

        List<Asset> assets = createAssets();

        Random r = new Random();
        List<ThreeDNode> listResults = new ArrayList<>();
        Optional<ThreeDModel> model = super.map3D.entrySet().stream().map(Map.Entry::getKey).findFirst();
        List<ThreeDModelRevision> revisions = super.map3D.get(model.get());
        getListNodes(listResults, model, revisions);
        while (listResults.size() == 0) {
            Thread.sleep(2000); // wait for eventual consistency
            getListNodes(listResults, model, revisions);
        }

        List<ThreeDAssetMapping> items = new ArrayList<>();
        for (Asset asset : assets) {
            ThreeDAssetMapping.Builder mappingBuilder = ThreeDAssetMapping.newBuilder();
            Integer position = r.nextInt(listResults.size());
            mappingBuilder.setAssetId(asset.getId());
            mappingBuilder.setNodeId(listResults.get(position).getId());
            items.add(mappingBuilder.build());
        }

        List<ThreeDAssetMapping> listCreated = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .create(model.get().getId(), revisions.get(0).getId(), items);
        assertEquals(items.size(), listCreated.size());

        LOG.info(loggingPrefix + "Start deleting 3D Asset Mappings.");
        Boolean isDeleted = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .delete(model.get().getId(), revisions.get(0).getId(), listCreated);
        LOG.info(loggingPrefix + "Finished deleting 3D Asset Mappings. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        assertTrue(isDeleted);

        LOG.info(loggingPrefix + "Finished writing 3D Asset Mappings. Duration : {}",
                Duration.between(startInstant, Instant.now()));
    }

    private void getListNodes(List<ThreeDNode> listResults, Optional<ThreeDModel> model, List<ThreeDModelRevision> revisions) throws Exception {
        client.threeD()
                .models()
                .revisions()
                .nodes()
                .list(model.get().getId(), revisions.get(0).getId())
                .forEachRemaining(val -> listResults.addAll(val));
    }

    private List<Asset> createAssets() throws Exception {
        Request request = Request.create().withRootParameter("limit", 1);
        Iterator<List<Asset>> it = client.assets().list(request);
        List<Asset> list = it.next();
        if (!list.isEmpty()) {
            return list;
        }

        Instant startInstant = Instant.now();
        LOG.info("Start upserting assets.");
        List<Asset> upsertAssetsList = DataGenerator.generateAssetHierarchy(COUNT_TO_BE_CREATE);
        client.assets().upsert(upsertAssetsList);
        LOG.info("Finished upserting assets. Duration: {}",
                Duration.between(startInstant, Instant.now()));
        return upsertAssetsList;
    }

    //Tests with PUBLIC DATA API-KEY

    @Test
    @Tag("remoteCDP")
    public void testListPublicData() throws Exception {
        CogniteClient client = getCogniteClientAPIKey();
        Iterator<List<ThreeDAssetMapping>> itFilter = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .list(PUBLIC_DATA_MODEL_ID, PUBLIC_DATA_REVISION_ID);
        List<ThreeDAssetMapping> listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);
    }

    @Test
    @Tag("remoteCDP")
    public void testListWithNodeIdPublicData() throws Exception {
        Request request = Request.create()
                .withRootParameter("limit", 1);
        CogniteClient client = getCogniteClientAPIKey();
        Iterator<List<ThreeDAssetMapping>> itFilter = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .list(PUBLIC_DATA_MODEL_ID, PUBLIC_DATA_REVISION_ID, request);
        List<ThreeDAssetMapping> listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);

        Random r = new Random();
        Integer position = r.nextInt(listResults.size());
        ThreeDAssetMapping assetMapping = listResults.get(position);
        request = Request.create()
                .withRootParameter("nodeId", assetMapping.getNodeId());
        itFilter = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .list(PUBLIC_DATA_MODEL_ID, PUBLIC_DATA_REVISION_ID, request);
        listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);
        assertEquals(listResults.get(0).getNodeId(), assetMapping.getNodeId());
    }

    @Test
    @Tag("remoteCDP")
    public void testListWithAssetIdPublicData() throws Exception {
        Request request = Request.create()
                .withRootParameter("limit", 1);
        CogniteClient client = getCogniteClientAPIKey();
        Iterator<List<ThreeDAssetMapping>> itFilter = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .list(PUBLIC_DATA_MODEL_ID, PUBLIC_DATA_REVISION_ID, request);
        List<ThreeDAssetMapping> listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);

        Random r = new Random();
        Integer position = r.nextInt(listResults.size());
        ThreeDAssetMapping assetMapping = listResults.get(position);
        request = Request.create()
                .withRootParameter("assetId", assetMapping.getAssetId());
        itFilter = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .list(PUBLIC_DATA_MODEL_ID, PUBLIC_DATA_REVISION_ID, request);
        listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);
        assertEquals(listResults.get(0).getAssetId(), assetMapping.getAssetId());
    }

    @Test
    @Tag("remoteCDP")
    public void testListWithIntersectsBoundingBoxAndLimitPublicData() throws Exception {
        Request request = Request.create()
                .withRootParameter("limit", 1);
        CogniteClient client = getCogniteClientAPIKey();
        Iterator<List<ThreeDAssetMapping>> itFilter = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .list(PUBLIC_DATA_MODEL_ID, PUBLIC_DATA_REVISION_ID, request);
        List<ThreeDAssetMapping> listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);

        Random r = new Random();
        Integer position = r.nextInt(listResults.size());
        ThreeDAssetMapping assetMapping = listResults.get(position);
        request = Request.create()
                .withRootParameter("intersectsBoundingBox", createBoundingBox());
        itFilter = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .list(PUBLIC_DATA_MODEL_ID, PUBLIC_DATA_REVISION_ID, request);
        listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);
    }

    @Test
    @Tag("remoteCDP")
    public void testFilterPublicData() throws Exception {
        CogniteClient client = getCogniteClientAPIKey();
        Iterator<List<ThreeDAssetMapping>> itFilter = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .filter(PUBLIC_DATA_MODEL_ID, PUBLIC_DATA_REVISION_ID);
        List<ThreeDAssetMapping> listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);
    }

    @Test
    @Tag("remoteCDP")
    public void testFilterWithAssetIdsPublicData() throws Exception {
        Request request = Request.create()
                .withRootParameter("limit", 100);
        CogniteClient client = getCogniteClientAPIKey();
        Iterator<List<ThreeDAssetMapping>> itFilter = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .list(PUBLIC_DATA_MODEL_ID, PUBLIC_DATA_REVISION_ID, request);
        List<ThreeDAssetMapping> listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);

        Random r = new Random();
        Integer position = r.nextInt(listResults.size());
        ThreeDAssetMapping assetMapping = listResults.get(position);
        Integer position2 = r.nextInt(listResults.size());
        ThreeDAssetMapping assetMapping2 = listResults.get(position2);

        Request request1 = Request.create()
                .withFilterParameter("assetIds", List.of(assetMapping.getAssetId(), assetMapping2.getAssetId()));
        Iterator<List<ThreeDAssetMapping>> itFilter1 = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .filter(PUBLIC_DATA_MODEL_ID, PUBLIC_DATA_REVISION_ID, request1);
        List<ThreeDAssetMapping> listResults1 = itFilter1.next();
        assertNotNull(listResults1);
        assertTrue(listResults1.size() > 0);
        listResults1.forEach(item -> {
            assertTrue(validate((v1) -> v1.equals(assetMapping.getAssetId()) || v1.equals(assetMapping2.getAssetId()), item.getAssetId()));
        });
    }

    @Test
    @Tag("remoteCDP")
    public void testFilterWithNodeIdsPublicData() throws Exception {
        Request request = Request.create()
                .withRootParameter("limit", 100);
        CogniteClient client = getCogniteClientAPIKey();
        Iterator<List<ThreeDAssetMapping>> itFilter = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .list(PUBLIC_DATA_MODEL_ID, PUBLIC_DATA_REVISION_ID, request);
        List<ThreeDAssetMapping> listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);

        Random r = new Random();
        Integer position = r.nextInt(listResults.size());
        ThreeDAssetMapping assetMapping = listResults.get(position);
        Integer position2 = r.nextInt(listResults.size());
        ThreeDAssetMapping assetMapping2 = listResults.get(position2);

        Request request1 = Request.create()
                .withFilterParameter("nodeIds", List.of(assetMapping.getNodeId(), assetMapping2.getNodeId()));
        Iterator<List<ThreeDAssetMapping>> itFilter1 = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .filter(PUBLIC_DATA_MODEL_ID, PUBLIC_DATA_REVISION_ID, request1);
        List<ThreeDAssetMapping> listResults1 = itFilter1.next();
        assertNotNull(listResults1);
        assertTrue(listResults1.size() > 0);
        assertTrue(listResults1.containsAll(List.of(assetMapping, assetMapping2)));
    }

    @Test
    @Tag("remoteCDP")
    public void testFilterWithTreeIndexesPublicData() throws Exception {
        Request request = Request.create()
                .withRootParameter("limit", 100);
        CogniteClient client = getCogniteClientAPIKey();
        Iterator<List<ThreeDAssetMapping>> itFilter = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .list(PUBLIC_DATA_MODEL_ID, PUBLIC_DATA_REVISION_ID, request);
        List<ThreeDAssetMapping> listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);

        Random r = new Random();
        Integer position = r.nextInt(listResults.size());
        ThreeDAssetMapping assetMapping = listResults.get(position);
        Integer position2 = r.nextInt(listResults.size());
        ThreeDAssetMapping assetMapping2 = listResults.get(position2);

        Request request1 = Request.create()
                .withFilterParameter("treeIndexes", List.of(assetMapping.getTreeIndex(), assetMapping2.getTreeIndex()));
        Iterator<List<ThreeDAssetMapping>> itFilter1 = client.threeD()
                .models()
                .revisions()
                .assetMappings()
                .filter(PUBLIC_DATA_MODEL_ID, PUBLIC_DATA_REVISION_ID, request1);
        List<ThreeDAssetMapping> listResults1 = itFilter1.next();
        assertNotNull(listResults1);
        assertTrue(listResults1.size() > 0);
        assertTrue(listResults1.containsAll(List.of(assetMapping, assetMapping2)));
    }

    private Boolean validate(Function<Long, Boolean> function, Long value) {
        return function.apply(value);
    }

    public ThreeDNode.BoundingBox createBoundingBox() {
        ThreeDNode.BoundingBox.Builder builder = ThreeDNode.BoundingBox.newBuilder();
        builder.addMin(62.64287567138672);
        builder.addMin(47.26144790649414);
        builder.addMin(-74.95000457763672);
        builder.addMax(214.71351623535156);
        builder.addMax(191.49485778808594);
        builder.addMax(125.31800079345703);
        return builder.build();
    }


}
