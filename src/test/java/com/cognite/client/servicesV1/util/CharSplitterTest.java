package com.cognite.client.servicesV1.util;

import java.util.Collection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CharSplitterTest {
  private static final CharSplitter splitter = new CharSplitter();

  @Test
  void testSimpleString() {
    final Collection<String> split = splitter.split(',', "A,B");
    assertEquals(2, split.size());
    assertTrue(split.contains("A"));
    assertTrue(split.contains("B"));
  }

  @Test
  void testSpecialDot() {
    final Collection<String> split = splitter.split('.', "A.B");
    assertEquals(2, split.size());
    assertTrue(split.contains("A"));
    assertTrue(split.contains("B"));
  }

  @Test
  void testSpecialStar() {
    final Collection<String> split = splitter.split('*', "A*B");
    assertEquals(2, split.size());
    assertTrue(split.contains("A"));
    assertTrue(split.contains("B"));
  }

  @Test
  void testMaskString() {
    final Collection<String> split = splitter.split(';', "A;\"B;C\"");
    assertEquals(2, split.size());
    assertTrue(split.contains("A"));
    assertTrue(split.contains("B;C"));
  }

  @Test
  void testEmptyString() {
    final Collection<String> split = splitter.split(',', "");
    assertEquals(0, split.size());
  }

  @Test
  void testEmptyItems() {
    assertEquals(2, splitter.split(',', ",").size());
    assertEquals(3, splitter.split(',', ",a,").size());
    assertEquals(3, splitter.split(';', ";;").size());
    assertEquals(2, splitter.split(';', ";\";\"").size());
  }

}