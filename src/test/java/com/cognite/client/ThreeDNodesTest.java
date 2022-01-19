package com.cognite.client;

import com.cognite.client.dto.ThreeDAvailableOutput;
import com.cognite.client.dto.ThreeDModel;
import com.cognite.client.dto.ThreeDModelRevision;
import com.cognite.client.dto.ThreeDNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

        LOG.info(loggingPrefix + "Finished list 3D Nodes. Duration: {}",
                Duration.between(startInstant, Instant.now()));
    }

}
