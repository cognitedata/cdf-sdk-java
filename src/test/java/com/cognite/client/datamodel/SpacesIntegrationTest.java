package com.cognite.client.datamodel;

import com.cognite.client.CogniteClient;
import com.cognite.client.TestConfigProvider;
import com.cognite.client.dto.datamodel.Space;
import com.cognite.client.dto.datamodel.SpaceReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpacesIntegrationTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void writeEditAndDeleteSpaces() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - writeEditAndDeleteSpaces() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = TestConfigProvider.getCogniteClient()
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start upserting spaces.");
        List<Space> upsertSpacesList = List.of(
                Space.newBuilder()
                        .setSpace("unit-test-space")
                        .setName("my-test-space")
                        .setDescription("a beautiful unit test space.")
                        .build()
        );
        List<Space> upsertedSpaces = client.experimental().dataModeling().spaces().upsert(upsertSpacesList);
        LOG.info(loggingPrefix + "Finished upserting {} spaces. Duration: {}",
                upsertSpacesList.size(),
                Duration.between(startInstant, Instant.now()));

        Thread.sleep(1000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start updating spaces.");
        List<Space> editedLabelsInput = upsertedSpaces.stream()
                .map(space -> space.toBuilder()
                        .setDescription("new-value")
                        .build())
                .toList();

        List<Space> spacesUpdateResults = client.experimental().dataModeling().spaces().upsert(editedLabelsInput);
        LOG.info(loggingPrefix + "Finished updating spaces. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        Thread.sleep(3000); // wait for eventual consistency

        LOG.info(loggingPrefix + "Start reading spaces.");
        List<Space> listSpacesResults = new ArrayList<>();
        client.experimental().dataModeling().spaces()
                .list()
                .forEachRemaining(labels -> listSpacesResults.addAll(labels));
        LOG.info(loggingPrefix + "Finished reading {} spaces. Duration: {}",
                listSpacesResults.size(),
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Start deleting spaces.");
        List<String> deleteSpacesInput = new ArrayList<>();
        listSpacesResults.stream()
                .filter(space -> space.getSpace().startsWith("unit-test"))
                .map(Space::getSpace)
                .forEach(item -> deleteSpacesInput.add(item));

        List<SpaceReference> deleteSpacesResults = client.experimental()
                .dataModeling()
                .spaces()
                .delete(deleteSpacesInput.toArray(new String[0]));
        LOG.info(loggingPrefix + "Finished deleting {} spaces. Duration: {}",
                deleteSpacesResults.size(),
                Duration.between(startInstant, Instant.now()));



        BooleanSupplier updateCondition = () -> {
            for (Space space : spacesUpdateResults)  {
                if (space.getDescription().equals("new-value")) {
                    // all good
                } else {
                    return false;
                }
            }
            return true;
        };

        assertTrue(updateCondition, "Labels update not correct");

        assertEquals(upsertSpacesList.size(), listSpacesResults.size());
        assertEquals(deleteSpacesInput.size(), deleteSpacesResults.size());
    }

}