package com.cognite.client.util;
import com.google.protobuf.Struct;
import com.google.protobuf.util.Structs;
import com.google.protobuf.util.Values;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParseStructTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    void parseStruct() {
        Instant startInstant = Instant.now();

        String loggingPrefix = "UnitTest - parseStruct() -";
        LOG.info(loggingPrefix + "Start test. Build Struct object.--------------------");
        Struct struct = Structs.of(
                "to_mySubResultObject", Values.of(Structs.of(
                        "results", Values.of(List.of(
                                Values.of(Structs.of(
                                        "userStatus", Values.of("one"),
                                        "systemStatus", Values.of("systemOne")
                                )),
                                Values.of(Structs.of(
                                        "userStatus", Values.of("two"),
                                        "systemStatus", Values.of("systemTwo")
                                ))
                        ))
                )),
                "nonRelevantField", Values.of("nonRelevantValue")
        );

        LOG.info(loggingPrefix + "Finished creating the Struct object. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "Parse Struct object.--------------------");

        List<String> result = ParseStruct.parseStringList(struct,
                List.of("to_mySubResultObject", "results", "userStatus"));
        LOG.info(loggingPrefix + "Result parse string list: {}", result);
        LOG.info(loggingPrefix + "Result parse string delimited: {}",
                ParseStruct.parseStringDelimited(struct, "to_mySubResultObject.results.userStatus", ";"));

        LOG.info(loggingPrefix + "Finished parsing the Struct object. Duration : {}",
                Duration.between(startInstant, Instant.now()));

        assertEquals(result, List.of("one", "two"));
    }
}