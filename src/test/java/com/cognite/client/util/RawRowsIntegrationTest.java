package com.cognite.client.util;

import com.cognite.client.dto.RawRow;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RawRowsIntegrationTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    void rawRowFromMap() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - rawRowFromMap() -";
        String dbName = "test-dbName";
        String tableName = "test-tableName";
        LOG.info(loggingPrefix + "Start test. Build columns maps.");
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> columnMapsList = generateColumnsMaps(1000);

        System.out.println("map[0]: " + mapper.writeValueAsString(columnMapsList.get(0)));
        LOG.info(loggingPrefix + "Finished building the columns maps. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "Start test. Build RawRow objects from maps.");
        List<RawRow> rowsList = new ArrayList<>();
        for (Map<String, Object> columnsMap : columnMapsList) {
            rowsList.add(RawRows.of(dbName, tableName, RandomStringUtils.randomAlphanumeric(5), columnsMap));
        }
        System.out.println("rows[0]: " + rowsList.get(0));
        LOG.info(loggingPrefix + "Finished building RawRow objects. Duration : {}",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        for (int i = 0; i < rowsList.size(); i++) {
            assertTrue(equals(rowsList.get(i).getColumns(), columnMapsList.get(i)));
        }
    }

    private List<Map<String, Object>> generateColumnsMaps(int noObjects) {
        Preconditions.condition(noObjects > 0, "Number of objects must be >0");
        List<Map<String, Object>> returnList = new ArrayList<>();
        for (int i = 0; i < noObjects; i++) {
            returnList.add(
                    Map.of(
                            "string-value", "my name value -" + RandomStringUtils.randomAlphanumeric(5),
                            "int-value", ThreadLocalRandom.current().nextInt(1, 200),
                            "double-value", ThreadLocalRandom.current().nextDouble(0, 100d),
                            "nested-object", Map.of(
                                    "sub-name", "my sub-name value -" + RandomStringUtils.randomAlphanumeric(5),
                                    "sub-type", "a good sub-type"
                            ),
                            "num-list", List.of(1, 2, 3),
                            "string-list", List.of("one", "two", "three")
                    )
            );
        }
        return returnList;
    }

    private boolean equals(Struct structColumns, Map<String, Object> mapColumns) {
        if (null == structColumns || null == mapColumns) return false;
        if (structColumns.getFieldsCount() != mapColumns.size()) return false;
        Map<String, Value> structColumnsMap = structColumns.getFieldsMap();

        return structColumnsMap.get("string-value").getStringValue().equals((String) mapColumns.get("string-value"))
                && Double.compare(structColumnsMap.get("int-value").getNumberValue(), Integer.valueOf((int) mapColumns.get("int-value")).doubleValue()) == 0
                && Double.compare(structColumnsMap.get("double-value").getNumberValue(), (double) mapColumns.get("double-value")) == 0
                && structColumnsMap.get("nested-object").getStructValue().getFieldsCount() == ((Map) mapColumns.get("nested-object")).size()
                && structColumnsMap.get("num-list").getListValue().getValuesList().size() == ((List) mapColumns.get("num-list")).size()
                && structColumnsMap.get("string-list").getListValue().getValuesList().size() == ((List) mapColumns.get("string-list")).size();
    }
}