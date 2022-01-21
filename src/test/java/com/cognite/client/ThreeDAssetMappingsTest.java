package com.cognite.client;

import com.cognite.client.dto.ThreeDAssetMapping;
import com.cognite.client.dto.ThreeDNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ThreeDAssetMappingsTest extends ThreeDBaseTest {

    public static final long PUBLIC_DATA_MODEL_ID = 3356984403684032l;
    public static final long PUBLIC_DATA_REVISION_ID = 6664823881595566l;
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Override
    Logger getLogger() {
        return LOG;
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
