package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.DataSet;
import com.cognite.client.dto.Item;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DataSetsIntegrationTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void listAndRetrieveDataSets() throws Exception {
        Instant startInstant = Instant.now();
        ClientConfig config = ClientConfig.create()
                .withNoWorkers(1)
                .withNoListPartitions(1);
        String loggingPrefix = "UnitTest - listAndRetrieveDataSets() -";
        LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
        CogniteClient client = TestConfigProvider.getCogniteClient()
                //.withClientConfig(config)
                ;
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        try {
            LOG.info(loggingPrefix + "Start listing data sets.");
            List<DataSet> listDataSetsResults = new ArrayList<>();
            client.datasets()
                    .list(Request.create())
                    .forEachRemaining(batch -> listDataSetsResults.addAll(batch));
            LOG.info(loggingPrefix + "Finished listing data sets. No results: {}. Duration: {}",
                    listDataSetsResults.size(),
                    Duration.between(startInstant, Instant.now()));


            LOG.info(loggingPrefix + "Start retrieving data sets.");
            List<Item> retrieveInput = listDataSetsResults.stream()
                    .map(dataSet -> {
                        if (dataSet.hasExternalId()) {
                            return Item.newBuilder()
                                    .setExternalId(dataSet.getExternalId())
                                    .build();
                        } else {
                            return Item.newBuilder()
                                    .setId(dataSet.getId())
                                    .build();
                        }
                    })
                    .collect(Collectors.toList());
            List<DataSet> retrieveDataSetResults = client.datasets()
                    .retrieve(retrieveInput);
            LOG.info(loggingPrefix + "Finished retrieving data sets. No results: {}. Duration: {}",
                    retrieveDataSetResults.size(),
                    Duration.between(startInstant, Instant.now()));


            assertEquals(listDataSetsResults.size(), retrieveDataSetResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

}