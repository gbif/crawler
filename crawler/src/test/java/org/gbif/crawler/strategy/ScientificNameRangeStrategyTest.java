/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.strategy;

import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScientificNameRangeStrategyTest {

  @Test
  public void testDefaultJob() {
    ScientificNameRangeStrategy strategy =
        new ScientificNameRangeStrategy(new ScientificNameRangeCrawlContext());
    ScientificNameRangeCrawlContext next = strategy.next();

    // Test if ...Aaa works
    assertEquals(0, next.getOffset());
    assertFalse(next.getLowerBound().isPresent());
    assertTrue(next.getUpperBound().isPresent());
    assertEquals("Aaa", next.getUpperBound().get());

    // Aaa...Aba
    assertTrue(strategy.hasNext());
    next = strategy.next();
    assertEquals(0, next.getOffset());
    assertTrue(next.getLowerBound().isPresent());
    assertEquals("Aaa", next.getLowerBound().get());
    assertTrue(next.getUpperBound().isPresent());
    assertEquals("Aba", next.getUpperBound().get());

    // Aza...Baa
    next.setLowerBound("Aya");
    next.setUpperBound("Aza");
    assertTrue(strategy.hasNext());
    next = strategy.next();
    assertEquals(0, next.getOffset());
    assertTrue(next.getLowerBound().isPresent());
    assertEquals("Aza", next.getLowerBound().get());
    assertTrue(next.getUpperBound().isPresent());
    assertEquals("Baa", next.getUpperBound().get());

    // Zxa...Zya, Zya...Zza
    next.setLowerBound("Zxa");
    next.setUpperBound("Zya");
    assertTrue(strategy.hasNext());
    next = strategy.next();
    assertEquals(0, next.getOffset());
    assertTrue(next.getLowerBound().isPresent());
    assertEquals("Zya", next.getLowerBound().get());
    assertTrue(next.getUpperBound().isPresent());
    assertEquals("Zza", next.getUpperBound().get());

    // Zza...
    assertTrue(strategy.hasNext());
    next = strategy.next();
    assertEquals(0, next.getOffset());
    assertTrue(next.getLowerBound().isPresent());
    assertEquals("Zza", next.getLowerBound().get());
    assertFalse(next.getUpperBound().isPresent());

    // Zza...
    assertTrue(strategy.hasNext());
    next = strategy.next();
    assertEquals(0, next.getOffset());
    assertFalse(next.getLowerBound().isPresent());
    assertFalse(next.getUpperBound().isPresent());

    assertFalse(strategy.hasNext());
  }

  /**
   * Tests that when instructed it simplified to Aaa-Baa, Baa-Caa instead of Aaa, Aba, Aca etc. Is
   * should still use 3 characters, but broader ranges.
   */
  @Test
  public void testModeABCJob() {
    ScientificNameRangeStrategy strategy =
        new ScientificNameRangeStrategy(
            new ScientificNameRangeCrawlContext(), ScientificNameRangeStrategy.Mode.ABC);
    ScientificNameRangeCrawlContext next = strategy.next();

    // Test if ...Aaa works
    assertEquals(0, next.getOffset());
    assertFalse(next.getLowerBound().isPresent());
    assertTrue(next.getUpperBound().isPresent());
    assertEquals("Aaa", next.getUpperBound().get());

    // Aaa...Baa
    assertTrue(strategy.hasNext());
    next = strategy.next();
    assertEquals(0, next.getOffset());
    assertTrue(next.getLowerBound().isPresent());
    assertEquals("Aaa", next.getLowerBound().get());
    assertTrue(next.getUpperBound().isPresent());
    assertEquals("Baa", next.getUpperBound().get());

    // Xaa...Yaa, Yaa...Zaa
    next.setLowerBound("Xaa");
    next.setUpperBound("Yaa");
    assertTrue(strategy.hasNext());
    next = strategy.next();
    assertEquals(0, next.getOffset());
    assertTrue(next.getLowerBound().isPresent());
    assertEquals("Yaa", next.getLowerBound().get());
    assertTrue(next.getUpperBound().isPresent());
    assertEquals("Zaa", next.getUpperBound().get());

    // Zaa...
    assertTrue(strategy.hasNext());
    next = strategy.next();
    assertEquals(0, next.getOffset());
    assertTrue(next.getLowerBound().isPresent());
    assertEquals("Zaa", next.getLowerBound().get());
    assertFalse(next.getUpperBound().isPresent());

    // null
    assertTrue(strategy.hasNext());
    next = strategy.next();
    assertEquals(0, next.getOffset());
    assertFalse(next.getLowerBound().isPresent());
    assertFalse(next.getUpperBound().isPresent());

    assertFalse(strategy.hasNext());
  }

  /**
   * Tests that when instructed it simplified to Aaa-Zaa. Is should still use 3 characters, but
   * should result in null-Aaa, Aaa-Zaa, Zaa-null only
   */
  @Test
  public void testModeAZJob() {
    ScientificNameRangeStrategy strategy =
        new ScientificNameRangeStrategy(
            new ScientificNameRangeCrawlContext(), ScientificNameRangeStrategy.Mode.AZ);
    ScientificNameRangeCrawlContext next = strategy.next();

    // Test if ...Aaa works
    assertEquals(0, next.getOffset());
    assertFalse(next.getLowerBound().isPresent());
    assertTrue(next.getUpperBound().isPresent());
    assertEquals("Aaa", next.getUpperBound().get());

    // Aaa...Zaa
    assertTrue(strategy.hasNext());
    next = strategy.next();
    assertEquals(0, next.getOffset());
    assertTrue(next.getLowerBound().isPresent());
    assertEquals("Aaa", next.getLowerBound().get());
    assertTrue(next.getUpperBound().isPresent());
    assertEquals("Zaa", next.getUpperBound().get());

    // Zaa...
    assertTrue(strategy.hasNext());
    next = strategy.next();
    assertEquals(0, next.getOffset());
    assertTrue(next.getLowerBound().isPresent());
    assertEquals("Zaa", next.getLowerBound().get());
    assertFalse(next.getUpperBound().isPresent());

    // null
    assertTrue(strategy.hasNext());
    next = strategy.next();
    assertEquals(0, next.getOffset());
    assertFalse(next.getLowerBound().isPresent());
    assertFalse(next.getUpperBound().isPresent());

    assertFalse(strategy.hasNext());
  }

  @Test
  public void testFailure() {
    ScientificNameRangeStrategy strategy =
        new ScientificNameRangeStrategy(new ScientificNameRangeCrawlContext());
    ScientificNameRangeCrawlContext next = strategy.next();

    next.setLowerBoundAbsent();
    next.setUpperBoundAbsent();
    // Make sure there are no more
    assertFalse(strategy.hasNext());
    assertThrows(NoSuchElementException.class, strategy::next);
  }
}
