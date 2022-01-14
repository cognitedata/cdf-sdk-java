package com.cognite.client;

import com.cognite.client.dto.Item;
import com.cognite.client.dto.ThreeDModel;
import com.cognite.client.dto.ThreeDModelRevision;
import com.cognite.client.dto.ThreeDNode;
import com.cognite.client.util.DataGenerator;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
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
                assertNotNull(listResults);
                validateFields(listResults);
            }
        }

        LOG.info(loggingPrefix + "Finished list 3D Nodes. Duration: {}",
                Duration.between(startInstant, Instant.now()));
    }

    @Test
    @Tag("remoteCDP")
    void retriveThreeDNodes() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "listThreeDNodes - ";
        LOG.info(loggingPrefix + "Start list 3D Nodes");

        for (Map.Entry<ThreeDModel, List<ThreeDModelRevision>> entry : super.map3D.entrySet()) {
            ThreeDModel model = entry.getKey();
            for (ThreeDModelRevision revision : entry.getValue()) {
                List<ThreeDNode> listResults =
                        client.threeD()
                                .models()
                                .revisions()
                                .nodes()
                                .retrieve(model.getId(), revision.getId());
                assertNotNull(listResults);
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
                assertNotNull(listResults);
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
                assertNotNull(listResultsAncestorNodes);
                validateFields(listResultsAncestorNodes);
            }
        }
        LOG.info(loggingPrefix + "Finished list 3D Ancestor Nodes. Duration: {}",
                Duration.between(startInstant, Instant.now()));
    }

    @Test
    @Tag("remoteCDP")
    void retriveThreeDNodesAncestorNodes() throws Exception {
        Thread.sleep(5000); // wait for eventual consistency
        Instant startInstant = Instant.now();
        String loggingPrefix = "listThreeDNodesAncestorNodes - ";
        LOG.info(loggingPrefix + "Start list 3D Ancestor Nodes");

        Random r = new Random();
        for (Map.Entry<ThreeDModel, List<ThreeDModelRevision>> entry : super.map3D.entrySet()) {
            ThreeDModel model = entry.getKey();
            for (ThreeDModelRevision revision : entry.getValue()) {
                List<ThreeDNode> listResults =
                        client.threeD()
                                .models()
                                .revisions()
                                .nodes()
                                .retrieve(model.getId(), revision.getId());
                assertNotNull(listResults);
                validateFields(listResults);

                Integer position = r.nextInt(listResults.size());
                ThreeDNode nodeDrawn = listResults.get(position);
                List<ThreeDNode> listResultsAncestorNodes =
                        client.threeD()
                                .models()
                                .revisions()
                                .nodes()
                                .retrieve(model.getId(), revision.getId(), nodeDrawn.getId());
                assertNotNull(listResultsAncestorNodes);
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
                List<ThreeDNode> listResults =
                        client.threeD()
                                .models()
                                .revisions()
                                .nodes()
                                .retrieve(model.getId(), revision.getId());
                assertNotNull(listResults);
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
                assertNotNull(nodesByIds);
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
                        .withFilterParameter("properties", createProperties());

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

    private ThreeDNode.Properties createProperties() {
        ListValue.Builder listBoxBu = ListValue.newBuilder();
        listBoxBu.addValues(Value.newBuilder().setNumberValue(1).build());
        listBoxBu.addValues(Value.newBuilder().setNumberValue(2).build());

        ListValue.Builder listCatBu = ListValue.newBuilder();
        listCatBu.addValues(Value.newBuilder().setStringValue("AB76").build());
        listCatBu.addValues(Value.newBuilder().setStringValue("AB77").build());

        Struct stBu = Struct.newBuilder()
                .putFields("boundingBox", Value.newBuilder().setListValue(listBoxBu.build()).build())
                .putFields("area", Value.newBuilder().setListValue(listCatBu.build()).build())
                .build();

        ThreeDNode.Properties.Builder propsBu = ThreeDNode.Properties.newBuilder();
        propsBu.setValues(stBu);

        return propsBu.build();
    }

//    private ThreeDNode.Properties createProperties() {
//        ThreeDNode.Properties.MapFieldEntry.MapFieldEntryValue.Builder mpfv = ThreeDNode.Properties.MapFieldEntry.MapFieldEntryValue.newBuilder();
//        mpfv.addValue("AB76");
//        mpfv.addValue("AB77");
//
//        ThreeDNode.Properties.MapFieldEntry.Builder mapBu = ThreeDNode.Properties.MapFieldEntry.newBuilder();
//        mapBu.putValues("Area", mpfv.build());
//
//        ThreeDNode.Properties.Builder propsBu = ThreeDNode.Properties.newBuilder();
//        propsBu.putValues("PDMS", mapBu.build());
//        return propsBu.build();
//    }

//    private ThreeDNode.Properties createProperties() {
//        ThreeDNode.Properties.Categories.CategoriesValues.Builder catValBu = ThreeDNode.Properties.Categories.CategoriesValues.newBuilder();
//        catValBu.addValue("AB76");
//
//        ThreeDNode.Properties.Categories.Builder categBu = ThreeDNode.Properties.Categories.newBuilder();
//        categBu.addValue(catValBu.build());
//
//        ThreeDNode.Properties.Builder propsBu = ThreeDNode.Properties.newBuilder();
//        propsBu.addCategories(categBu.build());
//        return propsBu.build();
//    }

//    private ThreeDNode.Properties createProperties() {
//        ThreeDNode.Categories.Builder catValBu = ThreeDNode.Categories.newBuilder();
//        catValBu.addValue("value1");
//
//        ThreeDNode.Properties.Builder propsBu = ThreeDNode.Properties.newBuilder();
//        propsBu.putCategories("cat1", catValBu.build());
//        return propsBu.build();
//    }

//    private ThreeDNode.Properties createProperties() {
//        ThreeDNode.Categories.Builder catValBu = ThreeDNode.Categories.newBuilder();
//        catValBu.addValue("");
//
//        ThreeDNode.Properties.Builder propsBu = ThreeDNode.Properties.newBuilder();
//        propsBu.addCategories(catValBu.build());
//        return propsBu.build();
//    }

//    private ThreeDNode.Properties createProperties() {
//        ThreeDNode.Categories.Builder catValBu = ThreeDNode.Categories.newBuilder();
//        catValBu.putValues("property1", "value1");
//
//        ThreeDNode.Properties.Builder propsBu = ThreeDNode.Properties.newBuilder();
//        propsBu.addCategories(catValBu.build());
//        return propsBu.build();
//    }

//    private ThreeDNode.Properties createProperties() {
//        ThreeDNode.Categories.CategoriesValues.Builder catValBu = ThreeDNode.Categories.CategoriesValues.newBuilder();
//        catValBu.putCategoriesValues("Type", "PIPE");
//
//        ThreeDNode.Categories.Builder catBu = ThreeDNode.Categories.newBuilder();
//        catBu.putValues("PDMS",catValBu.build());
//
//        ThreeDNode.Properties.Builder propsBu = ThreeDNode.Properties.newBuilder();
//        propsBu.addCategories(catBu.build());
//        return propsBu.build();
//    }

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
