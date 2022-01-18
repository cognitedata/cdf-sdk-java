package com.cognite.client;

import com.cognite.client.dto.Item;
import com.cognite.client.dto.ThreeDModel;
import com.cognite.client.dto.ThreeDModelRevision;
import com.cognite.client.dto.ThreeDNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ThreeDNodesTest extends ThreeDBaseTest {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Override
    Logger getLogger() {
        return LOG;
    }

    @Test
    @Tag("remoteCDP")
    void listThreeDNodes() throws Exception {
        Thread.sleep(5000); // wait for eventual consistency
        Instant startInstant = Instant.now();
        String loggingPrefix = "listThreeDNodes - ";
        LOG.info(loggingPrefix + "Start list 3D Nodes");

        for (Map.Entry<ThreeDModel, List<ThreeDModelRevision>> entry : super.map3D.entrySet()) {
            ThreeDModel model = entry.getKey();
            for (ThreeDModelRevision revision : entry.getValue()) {
                List<ThreeDNode> listResults = new ArrayList<>();
                        client.threeD()
                                .models()
                                .revisions()
                                .nodes()
                                .list(model.getId(), revision.getId())
                                .forEachRemaining(val -> listResults.addAll(val));
                validateList(listResults);
                validateFields(listResults);
            }
        }

        LOG.info(loggingPrefix + "Finished list 3D Nodes. Duration: {}",
                Duration.between(startInstant, Instant.now()));
    }

    @Test
    @Tag("remoteCDP")
    void listThreeDNodesAncestorNodes() throws Exception {
        Thread.sleep(5000); // wait for eventual consistency
        Instant startInstant = Instant.now();
        String loggingPrefix = "listThreeDNodesAncestorNodes - ";
        LOG.info(loggingPrefix + "Start list 3D Ancestor Nodes");

        Random r = new Random();
        for (Map.Entry<ThreeDModel, List<ThreeDModelRevision>> entry : super.map3D.entrySet()) {
            ThreeDModel model = entry.getKey();
            for (ThreeDModelRevision revision : entry.getValue()) {
                List<ThreeDNode> listResults = new ArrayList<>();
                        client.threeD()
                                .models()
                                .revisions()
                                .nodes()
                                .list(model.getId(), revision.getId())
                                .forEachRemaining(val -> listResults.addAll(val));
                validateList(listResults);
                validateFields(listResults);

                Integer position = r.nextInt(listResults.size());
                ThreeDNode nodeDrawn = listResults.get(position);
                List<ThreeDNode> listResultsAncestorNodes = new ArrayList<>();
                        client.threeD()
                                .models()
                                .revisions()
                                .nodes()
                                .list(model.getId(), revision.getId(), nodeDrawn.getId())
                                .forEachRemaining(val -> listResultsAncestorNodes.addAll(val));
                validateList(listResultsAncestorNodes);
                validateFields(listResultsAncestorNodes);
            }
        }
        LOG.info(loggingPrefix + "Finished list 3D Ancestor Nodes. Duration: {}",
                Duration.between(startInstant, Instant.now()));
    }

    @Test
    @Tag("remoteCDP")
    void getThreeDNodesByIds() throws Exception {
        Thread.sleep(10000); // wait for eventual consistency
        Instant startInstant = Instant.now();
        String loggingPrefix = "getThreeDNodesByIds - ";
        LOG.info(loggingPrefix + "Start getting 3D Nodes by ids");

        Random r = new Random();
        for (Map.Entry<ThreeDModel, List<ThreeDModelRevision>> entry : super.map3D.entrySet()) {
            ThreeDModel model = entry.getKey();
            for (ThreeDModelRevision revision : entry.getValue()) {
                List<ThreeDNode> listResults = new ArrayList<>();
                client.threeD()
                        .models()
                        .revisions()
                        .nodes()
                        .list(model.getId(), revision.getId())
                        .forEachRemaining(val -> listResults.addAll(val));
                validateList(listResults);
                validateFields(listResults);

                List<Item> tdList = new ArrayList<>();
                listResults.stream()
                        .map(td -> Item.newBuilder()
                                .setId(td.getId())
                                .build())
                        .forEach(item -> tdList.add(item));

                List<ThreeDNode> nodesByIds =
                        client.threeD()
                                .models()
                                .revisions()
                                .nodes()
                                .retrieve(model.getId(), revision.getId(), tdList);

                assertEquals(listResults.size(), nodesByIds.size());
                validateList(nodesByIds);
                validateFields(nodesByIds);
            }
        }
        LOG.info(loggingPrefix + "Finished getting 3D Nodes by ids. Duration: {}",
                Duration.between(startInstant, Instant.now()));
    }

    @Test
    @Tag("remoteCDP")
    void filterThreeDNodes() throws Exception {
        Thread.sleep(5000); // wait for eventual consistency
        Instant startInstant = Instant.now();
        String loggingPrefix = "filterThreeDNodes - ";
        LOG.info(loggingPrefix + "Start filter 3D Nodes");

        Random r = new Random();
        for (Map.Entry<ThreeDModel, List<ThreeDModelRevision>> entry : super.map3D.entrySet()) {
            ThreeDModel model = entry.getKey();
            for (ThreeDModelRevision revision : entry.getValue()) {

                Request request = Request.create()
                        .withFilterParameter("properties", createFilterPropertiesWithCategories());

                List<ThreeDNode> listResults = new ArrayList<>();
                client.threeD()
                        .models()
                        .revisions()
                        .nodes()
                        .filter(model.getId(), revision.getId(), request)
                        .forEachRemaining(val -> listResults.addAll(val));
                assertNotNull(listResults);
                validateFields(listResults);
            }
        }
        LOG.info(loggingPrefix + "Finished filter 3D Nodes. Duration: {}",
                Duration.between(startInstant, Instant.now()));
    }



    //Tests with PUBLIC DATA API-KEY

    @Test
    @Tag("remoteCDP")
    public void testListPublicData() throws Exception {
        client = getCogniteClientAPIKey();
        Iterator<List<ThreeDNode>> it = client.threeD()
                .models()
                .revisions()
                .nodes()
                .list(3356984403684032l, 6664823881595566l);
        List<ThreeDNode> listResults = it.next();
        assertNotNull(listResults);
    }

    @Test
    @Tag("remoteCDP")
    public void testFilterPublicData() throws Exception {
        client = getCogniteClientAPIKey();
        Request request = Request.create()
                .withFilterParameter("properties", createFilterPropertiesWithCategories());

        Iterator<List<ThreeDNode>> itFilter = client.threeD()
                .models()
                .revisions()
                .nodes()
                .filter(3356984403684032l, 6664823881595566l, request);
        List<ThreeDNode> listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);
    }

    @Test
    @Tag("remoteCDP")
    public void testFilter1CategorieAnd2ItemsPublicData() throws Exception {
        client = getCogniteClientAPIKey();
        Request request = Request.create()
                .withFilterParameter("properties", createFilterProperties1CategoriesAnd2Items());

        Iterator<List<ThreeDNode>> itFilter = client.threeD()
                .models()
                .revisions()
                .nodes()
                .filter(3356984403684032l, 6664823881595566l, request);
        List<ThreeDNode> listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);
    }

    @Test
    @Tag("remoteCDP")
    public void testFilterWith2ICategoriesPublicData() throws Exception {
        client = getCogniteClientAPIKey();
        Request request = Request.create()
                .withRootParameter("limit", 1)
                .withFilterParameter("properties", createFilterPropertiesWith2Categories());

        Iterator<List<ThreeDNode>> itFilter = client.threeD()
                .models()
                .revisions()
                .nodes()
                .filter(3356984403684032l, 6664823881595566l, request);
        List<ThreeDNode> listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);
    }

    @Test
    @Tag("remoteCDP")
    public void testFilterWith2ICategoriesAnd2ItemsPublicData() throws Exception {
        client = getCogniteClientAPIKey();
        Request request = Request.create()
                .withRootParameter("limit", 1)
                .withFilterParameter("properties", createFilterPropertiesWith2CategoriesANd2Items());

        Iterator<List<ThreeDNode>> itFilter = client.threeD()
                .models()
                .revisions()
                .nodes()
                .filter(3356984403684032l, 6664823881595566l, request);
        List<ThreeDNode> listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);
    }

    @Test
    @Tag("remoteCDP")
    public void testFilterEmptyPublicData() throws Exception {
        client = getCogniteClientAPIKey();

        Iterator<List<ThreeDNode>> itFilter = client.threeD()
                .models()
                .revisions()
                .nodes()
                .filter(3356984403684032l, 6664823881595566l);
        List<ThreeDNode> listResults = itFilter.next();
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);
    }

    private ThreeDNode.PropertiesFilter createFilterPropertiesWith2Categories() {
        ThreeDNode.PropertiesFilter.Categories.CategoriesValues.Builder catValBuilder =
                ThreeDNode.PropertiesFilter.Categories.CategoriesValues.newBuilder();
        catValBuilder.addValuesString("Group");

        ThreeDNode.PropertiesFilter.Categories.Builder catBuilder = ThreeDNode.PropertiesFilter.Categories.newBuilder();
        catBuilder.setName("Item");
        catBuilder.putValues("Type", catValBuilder.build());

        ThreeDNode.PropertiesFilter.Categories.CategoriesValues.Builder cat2ValBuilder =
                ThreeDNode.PropertiesFilter.Categories.CategoriesValues.newBuilder();
        cat2ValBuilder.addValuesString("PNOD");

        ThreeDNode.PropertiesFilter.Categories.Builder cat2Builder = ThreeDNode.PropertiesFilter.Categories.newBuilder();
        cat2Builder.setName("PDMS");
        cat2Builder.putValues("Type", cat2ValBuilder.build());

        ThreeDNode.PropertiesFilter.Builder propsBuilder = ThreeDNode.PropertiesFilter.newBuilder();
        propsBuilder.addCategories(catBuilder.build());
        propsBuilder.addCategories(cat2Builder.build());
        return propsBuilder.build();
    }

    private ThreeDNode.PropertiesFilter createFilterPropertiesWith2CategoriesANd2Items() {
        ThreeDNode.PropertiesFilter.Categories.CategoriesValues.Builder catValBuilder =
                ThreeDNode.PropertiesFilter.Categories.CategoriesValues.newBuilder();
        catValBuilder.addValuesString("Group");

        ThreeDNode.PropertiesFilter.Categories.CategoriesValues.Builder catValTwoBuilder =
                ThreeDNode.PropertiesFilter.Categories.CategoriesValues.newBuilder();
        catValTwoBuilder.addValuesString("false");

        ThreeDNode.PropertiesFilter.Categories.Builder catBuilder = ThreeDNode.PropertiesFilter.Categories.newBuilder();
        catBuilder.setName("Item");
        catBuilder.putValues("Type", catValBuilder.build());
        catBuilder.putValues("Required", catValTwoBuilder.build());


        ThreeDNode.PropertiesFilter.Categories.CategoriesValues.Builder cat2ValBuilder =
                ThreeDNode.PropertiesFilter.Categories.CategoriesValues.newBuilder();
        cat2ValBuilder.addValuesString("PNOD");

        ThreeDNode.PropertiesFilter.Categories.Builder cat2Builder = ThreeDNode.PropertiesFilter.Categories.newBuilder();
        cat2Builder.setName("PDMS");
        cat2Builder.putValues("Type", cat2ValBuilder.build());

        ThreeDNode.PropertiesFilter.Builder propsBuilder = ThreeDNode.PropertiesFilter.newBuilder();
        propsBuilder.addCategories(catBuilder.build());
        propsBuilder.addCategories(cat2Builder.build());
        return propsBuilder.build();
    }

    private ThreeDNode.PropertiesFilter createFilterProperties1CategoriesAnd2Items() {
        ThreeDNode.PropertiesFilter.Categories.CategoriesValues.Builder catValOneBuilder =
                ThreeDNode.PropertiesFilter.Categories.CategoriesValues.newBuilder();
        catValOneBuilder.addValuesString("Box");

        ThreeDNode.PropertiesFilter.Categories.CategoriesValues.Builder catValTwoBuilder =
                ThreeDNode.PropertiesFilter.Categories.CategoriesValues.newBuilder();
        catValTwoBuilder.addValuesString("false");

        ThreeDNode.PropertiesFilter.Categories.Builder catBuilder = ThreeDNode.PropertiesFilter.Categories.newBuilder();
        catBuilder.setName("Item");
        catBuilder.putValues("Type", catValOneBuilder.build());
        catBuilder.putValues("Required", catValTwoBuilder.build());

        ThreeDNode.PropertiesFilter.Builder propsBuilder = ThreeDNode.PropertiesFilter.newBuilder();
        propsBuilder.addCategories(catBuilder.build());
        return propsBuilder.build();
    }

    private ThreeDNode.PropertiesFilter createFilterPropertiesWithCategories() {
        ThreeDNode.PropertiesFilter.Categories.CategoriesValues.Builder catValBuilder =
                ThreeDNode.PropertiesFilter.Categories.CategoriesValues.newBuilder();
        catValBuilder.addValuesString("Box");

        ThreeDNode.PropertiesFilter.Categories.Builder catBuilder = ThreeDNode.PropertiesFilter.Categories.newBuilder();
        catBuilder.setName("Item");
        catBuilder.putValues("Type", catValBuilder.build());

        ThreeDNode.PropertiesFilter.Builder propsBuilder = ThreeDNode.PropertiesFilter.newBuilder();
        propsBuilder.addCategories(catBuilder.build());
        return propsBuilder.build();
    }

    private void validateList(List<ThreeDNode> listResults) {
        assertNotNull(listResults);
        assertTrue(listResults.size() > 0);
    }

    private void validateFields(List<ThreeDNode> listResults) {
        listResults.forEach(node -> {
            assertNotNull(node);
            assertNotNull(node.getId());
            assertNotNull(node.getDepth());
            assertNotNull(node.getName());
            assertFalse(node.getName().equals(""));
            assertNotNull(node.getParentId());
            assertNotNull(node.getSubtreeSize());
            assertNotNull(node.getTreeIndex());
            assertNotNull(node.getBoundingBox());
            assertNotNull(node.getBoundingBox().getMaxList());
            assertNotNull(node.getBoundingBox().getMinList());
        });
    }
}
