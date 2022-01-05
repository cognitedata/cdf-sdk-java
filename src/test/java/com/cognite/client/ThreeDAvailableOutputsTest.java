package com.cognite.client;


import com.cognite.client.dto.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ThreeDAvailableOutputsTest extends ThreeDBaseTest{

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Override
    Logger getLogger() {
        return LOG;
    }

    @Test
    @Tag("remoteCDP")
    void retrieveThreeDAvailableOutputs() throws MalformedURLException {
        try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "retrieveThreeDAvailableOutputs - ";
            LOG.info(loggingPrefix + "Start retrieving 3D Available Outputs.");

            List<ThreeDAvailableOutput> listResultsOutputs = new ArrayList<>();
            for (Map.Entry<ThreeDModel, List<ThreeDModelRevision>> entry : super.map3D.entrySet()) {
                ThreeDModel model = entry.getKey();
                for (ThreeDModelRevision revision : entry.getValue()) {
                    List<ThreeDAvailableOutput> listResults =
                            client.threeD()
                                    .models()
                                    .revisions()
                                    .availableOutputs()
                                    .retrieve(model.getId(), revision.getId());
                    listResultsOutputs.addAll(listResults);
                }
            }
            LOG.info(loggingPrefix + "Finished retrieving 3D Available Outputs. Duration : {}",
                    Duration.between(startInstant, Instant.now()));
        } catch (Exception e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }
}
