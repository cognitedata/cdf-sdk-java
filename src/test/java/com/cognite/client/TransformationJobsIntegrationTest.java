package com.cognite.client;

import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.DataSet;
import com.cognite.client.dto.Item;
import com.cognite.client.dto.Transformation;
import com.cognite.client.util.DataGenerator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TransformationJobsIntegrationTest {

    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private static final Integer COUNT_TO_BE_CREATE_TD = 1;

    @Test
    @Tag("remoteCDP")
    void writeRunCancelAndDelete() throws Exception {
        try {
            Instant startInstant = Instant.now();
            String loggingPrefix = "UnitTest - writeReadAndDelete() -";
            LOG.info(loggingPrefix + "Start test. Creating Cognite client.");
            CogniteClient client = getCogniteClient(startInstant, loggingPrefix);

            Long dataSetId = getOrCreateDataSet(startInstant, loggingPrefix, client);

            LOG.info(loggingPrefix + "------------ Start create Transformations. ------------------");
            List<Transformation> listToBeCreate = new ArrayList<>();
            List<Transformation> generatedWithDestinationDataSource1List =
                    DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, Transformation.Destination.DestinationType.DATA_SOURCE_1, 2,
                            TestConfigProvider.getClientId(),
                            TestConfigProvider.getClientSecret(),
                            TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()).toString(),
                            TestConfigProvider.getProject());
            List<Transformation> generatedWithDestinationRawDataSourceList = DataGenerator.generateTransformations(COUNT_TO_BE_CREATE_TD, dataSetId, Transformation.Destination.DestinationType.RAW_DATA_SOURCE, 2,
                    TestConfigProvider.getClientId(),
                    TestConfigProvider.getClientSecret(),
                    TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()).toString(),
                    TestConfigProvider.getProject());
            listToBeCreate.addAll(generatedWithDestinationDataSource1List);
            listToBeCreate.addAll(generatedWithDestinationRawDataSourceList);

            List<Transformation> createdList = client.transformation().upsert(listToBeCreate);
            LOG.info(loggingPrefix + "------------ Finished creating Transformations. Duration: {} -----------",
                    Duration.between(startInstant, Instant.now()));
            assertEquals(listToBeCreate.size(), createdList.size());

            runJobs(client, createdList);
            cancelJobs(client, createdList);

            LOG.info(loggingPrefix + "Start deleting Transformations.");
            List<Item> deleteItemsInput = new ArrayList<>();
            createdList.stream()
                    .map(tra -> Item.newBuilder()
                            .setExternalId(tra.getExternalId())
                            .build())
                    .forEach(item -> deleteItemsInput.add(item));
            List<Item> deleteItemsResults = client.transformation().delete(deleteItemsInput);
            LOG.info(loggingPrefix + "Finished deleting Transformations. Duration: {}",
                    Duration.between(startInstant, Instant.now()));
            assertEquals(deleteItemsInput.size(), deleteItemsResults.size());
        } catch (Exception e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }

    }

    private void runJobs(CogniteClient client, List<Transformation> createdList) {
        try {
            Transformation.Job jobRead1 =
                    client.transformation().jobs().run(createdList.get(0).getId());

            Transformation.Job jobRead2 =
                    client.transformation().jobs().run(createdList.get(1).getExternalId());
            assertNotNull(jobRead1);
            assertNotNull(jobRead2);
            assertTrue("Created".equals(jobRead1.getStatus()));
            assertTrue("Created".equals(jobRead2.getStatus()));
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("ERROR TRYING RUN TRANSFORMATION ", e);
        }
    }

    private void cancelJobs(CogniteClient client, List<Transformation> createdList) {
        try {
            Boolean jobResult1 =
                    client.transformation().jobs().cancel(createdList.get(0).getId());

            Boolean jobResult2 =
                    client.transformation().jobs().cancel(createdList.get(1).getExternalId());

            assertNotNull(jobResult1);
            assertNotNull(jobResult2);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("ERROR TRYING RUN TRANSFORMATION ", e);
        }
    }

    private CogniteClient getCogniteClient(Instant startInstant, String loggingPrefix) throws MalformedURLException {
        CogniteClient client = CogniteClient.ofClientCredentials(
                        TestConfigProvider.getClientId(),
                        TestConfigProvider.getClientSecret(),
                        TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                .withProject(TestConfigProvider.getProject())
                .withBaseUrl(TestConfigProvider.getHost());
        LOG.info(loggingPrefix + "Finished creating the Cognite client. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        return client;
    }

    private Long getOrCreateDataSet(Instant startInstant, String loggingPrefix, CogniteClient client) throws Exception {
        Request request = Request.create()
                .withRootParameter("limit", 1);
        Iterator<List<DataSet>> itDataSet = client.datasets().list(request);
        Long dataSetId = null;
        List<DataSet> list = itDataSet.next();
        if (list != null && list.size() > 0) {
            dataSetId = list.get(0).getId();
        } else {
            LOG.info(loggingPrefix + "------------ Start create or find one data set. ------------------");
            List<DataSet> upsertDataSetList = DataGenerator.generateDataSets(1);
            List<DataSet> upsertDataSetsResults = client.datasets().upsert(upsertDataSetList);
            dataSetId = upsertDataSetsResults.get(0).getId();
            LOG.info(loggingPrefix + "----------- Finished upserting data set. Duration: {} -------------",
                    Duration.between(startInstant, Instant.now()));
        }
        return dataSetId;
    }
}
