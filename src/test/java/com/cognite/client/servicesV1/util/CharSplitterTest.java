package com.cognite.client.servicesV1.util;

import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CharSplitterTest {
    private static final CharSplitter splitter = new CharSplitter();

    @Test
    void testSimpleString() throws Throwable {
        final Collection<String> split = splitter.split(',', "A,B");
        assertEquals(2, split.size());
        assertTrue(split.contains("A"));
        assertTrue(split.contains("B"));
    }

    @Test
    void testSpecialDot() throws Throwable {
        final Collection<String> split = splitter.split('.', "A.B");
        assertEquals(2, split.size());
        assertTrue(split.contains("A"));
        assertTrue(split.contains("B"));
    }

    @Test
    void testSpecialStar() throws Throwable {
        final Collection<String> split = splitter.split('*', "A*B");
        assertEquals(2, split.size());
        assertTrue(split.contains("A"));
        assertTrue(split.contains("B"));
    }

    @Test
    void testMaskString() throws Throwable {
        final Collection<String> split = splitter.split(';', "A;\"B;C\"");
        assertEquals(2, split.size());
        assertTrue(split.contains("A"));
        assertTrue(split.contains("B;C"));
    }

    @Test
    void testEmptyString() throws Throwable {
        final Collection<String> split = splitter.split(',', "");
        assertEquals(0, split.size());
    }

    @Test
    void testEmptyItems() throws Throwable {
        assertEquals(2, splitter.split(',', ",").size());
        assertEquals(3, splitter.split(',', ",a,").size());
        assertEquals(3, splitter.split(';', ";;").size());
        assertEquals(2, splitter.split(';', ";\";\"").size());
    }

}