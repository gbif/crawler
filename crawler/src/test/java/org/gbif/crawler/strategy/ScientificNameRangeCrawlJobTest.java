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

import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class ScientificNameRangeCrawlJobTest {

  @Test
  public void testDefaultConstructor() {
    ScientificNameRangeCrawlContext job = new ScientificNameRangeCrawlContext();

    assertThat(job.getLowerBound().isPresent()).isFalse();
    assertThat(job.getUpperBound().get()).isEqualTo("Aaa");
    assertThat(job.getOffset()).isZero();
  }

  @Test
  public void testCustomConstructor() {
    ScientificNameRangeCrawlContext job;

    job = new ScientificNameRangeCrawlContext(0, null, null);
    assertThat(job.getLowerBound().isPresent()).isFalse();
    assertThat(job.getUpperBound().isPresent()).isFalse();
    assertThat(job.getOffset()).isZero();

    job = new ScientificNameRangeCrawlContext(0, "aaa", null);
    assertThat(job.getLowerBound().isPresent()).isTrue();
    assertThat(job.getLowerBound().get()).isEqualTo("aaa");
    assertThat(job.getUpperBound().isPresent()).isFalse();
    assertThat(job.getOffset()).isZero();

    job = new ScientificNameRangeCrawlContext(10, "aaa", "zzz");
    assertThat(job.getLowerBound().isPresent()).isTrue();
    assertThat(job.getLowerBound().get()).isEqualTo("aaa");
    assertThat(job.getUpperBound().isPresent()).isTrue();
    assertThat(job.getUpperBound().get()).isEqualTo("zzz");
    assertThat(job.getOffset()).isEqualTo(10);

    try {
      new ScientificNameRangeCrawlContext(0, "", null);
      fail();
    } catch (IllegalArgumentException e) {
    }

    try {
      new ScientificNameRangeCrawlContext(-10, null, null);
      fail();
    } catch (IllegalArgumentException e) {
    }

    try {
      new ScientificNameRangeCrawlContext(0, null, "abcd");
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testSetAbsent() {
    ScientificNameRangeCrawlContext job = new ScientificNameRangeCrawlContext(0, "aaa", "zzz");

    job.setLowerBoundAbsent();
    assertThat(job.getLowerBound().isPresent()).isFalse();
    assertThat(job.getUpperBound().isPresent()).isTrue();

    job.setUpperBoundAbsent();
    assertThat(job.getLowerBound().isPresent()).isFalse();
    assertThat(job.getUpperBound().isPresent()).isFalse();
  }
}
