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

import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class ScientificNameRangeStrategyTest {

  @Test
  public void testDefaultJob() {
    ScientificNameRangeStrategy strategy =
        new ScientificNameRangeStrategy(new ScientificNameRangeCrawlContext());
    ScientificNameRangeCrawlContext next = strategy.next();

    // Test if ...Aaa works
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isFalse();
    assertThat(next.getUpperBound().isPresent()).isTrue();
    assertThat(next.getUpperBound().get()).isEqualTo("Aaa");

    // Aaa...Aba
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isTrue();
    assertThat(next.getLowerBound().get()).isEqualTo("Aaa");
    assertThat(next.getUpperBound().isPresent()).isTrue();
    assertThat(next.getUpperBound().get()).isEqualTo("Aba");

    // Aza...Baa
    next.setLowerBound("Aya");
    next.setUpperBound("Aza");
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isTrue();
    assertThat(next.getLowerBound().get()).isEqualTo("Aza");
    assertThat(next.getUpperBound().isPresent()).isTrue();
    assertThat(next.getUpperBound().get()).isEqualTo("Baa");

    // Zxa...Zya, Zya...Zza
    next.setLowerBound("Zxa");
    next.setUpperBound("Zya");
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isTrue();
    assertThat(next.getLowerBound().get()).isEqualTo("Zya");
    assertThat(next.getUpperBound().isPresent()).isTrue();
    assertThat(next.getUpperBound().get()).isEqualTo("Zza");

    // Zza...
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isTrue();
    assertThat(next.getLowerBound().get()).isEqualTo("Zza");
    assertThat(next.getUpperBound().isPresent()).isFalse();

    // Zza...
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isFalse();
    assertThat(next.getUpperBound().isPresent()).isFalse();

    assertThat(strategy.hasNext()).isFalse();
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
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isFalse();
    assertThat(next.getUpperBound().isPresent()).isTrue();
    assertThat(next.getUpperBound().get()).isEqualTo("Aaa");

    // Aaa...Baa
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isTrue();
    assertThat(next.getLowerBound().get()).isEqualTo("Aaa");
    assertThat(next.getUpperBound().isPresent()).isTrue();
    assertThat(next.getUpperBound().get()).isEqualTo("Baa");

    // Xaa...Yaa, Yaa...Zaa
    next.setLowerBound("Xaa");
    next.setUpperBound("Yaa");
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isTrue();
    assertThat(next.getLowerBound().get()).isEqualTo("Yaa");
    assertThat(next.getUpperBound().isPresent()).isTrue();
    assertThat(next.getUpperBound().get()).isEqualTo("Zaa");

    // Zaa...
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isTrue();
    assertThat(next.getLowerBound().get()).isEqualTo("Zaa");
    assertThat(next.getUpperBound().isPresent()).isFalse();

    // null
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isFalse();
    assertThat(next.getUpperBound().isPresent()).isFalse();

    assertThat(strategy.hasNext()).isFalse();
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
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isFalse();
    assertThat(next.getUpperBound().isPresent()).isTrue();
    assertThat(next.getUpperBound().get()).isEqualTo("Aaa");

    // Aaa...Zaa
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isTrue();
    assertThat(next.getLowerBound().get()).isEqualTo("Aaa");
    assertThat(next.getUpperBound().isPresent()).isTrue();
    assertThat(next.getUpperBound().get()).isEqualTo("Zaa");

    // Zaa...
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isTrue();
    assertThat(next.getLowerBound().get()).isEqualTo("Zaa");
    assertThat(next.getUpperBound().isPresent()).isFalse();

    // null
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isFalse();
    assertThat(next.getUpperBound().isPresent()).isFalse();

    assertThat(strategy.hasNext()).isFalse();
  }

  @Test
  public void testFailure() {
    ScientificNameRangeStrategy strategy =
        new ScientificNameRangeStrategy(new ScientificNameRangeCrawlContext());
    ScientificNameRangeCrawlContext next = strategy.next();

    next.setLowerBoundAbsent();
    next.setUpperBoundAbsent();
    // Make sure there are no more
    assertThat(strategy.hasNext()).isFalse();

    try {
      strategy.next();
      fail();
    } catch (NoSuchElementException e) {
    }
  }
}
