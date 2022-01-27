package com.cognite.client;

import com.cognite.client.dto.ThreeDModel;
import com.cognite.client.dto.ThreeDModelRevision;
import com.cognite.client.dto.ThreeDRevisionLog;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ThreeDRevisionLogIntegrationTest extends ThreeDBaseIntegrationTest {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Override
    Logger getLogger() {
        return LOG;
    }

    @Test
    @Tag("remoteCDP")
    void listThreeDRevisionLogs() throws Exception {
        try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "UnitTest - listThreeDModelsRevisions() - ";

            LOG.info(loggingPrefix + "Start list 3D Revision Logs.");

            List<ThreeDRevisionLog> listResultsLogs = new ArrayList<>();
            for (Map.Entry<ThreeDModel, List<ThreeDModelRevision>> entry : super.map3D.entrySet()) {
                ThreeDModel model = entry.getKey();
                for (ThreeDModelRevision revision : entry.getValue()) {
                    List<ThreeDRevisionLog> listResults =
                            client.threeD()
                                    .models()
                                    .revisions()
                                    .revisionLogs()
                                    .retrieve(model.getId(), revision.getId());
                    listResultsLogs.addAll(listResults);
                }
            }

            LOG.info(loggingPrefix + "Finished list 3D Revision Logs. Duration : {}",
                    Duration.between(startInstant, Instant.now()));
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }

    }



}
