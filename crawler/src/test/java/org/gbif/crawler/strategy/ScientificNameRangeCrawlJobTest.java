/*
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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScientificNameRangeCrawlJobTest {

  @Test
  public void testDefaultConstructor() {
    ScientificNameRangeCrawlContext job = new ScientificNameRangeCrawlContext();

    assertFalse(job.getLowerBound().isPresent());
    assertTrue(job.getUpperBound().isPresent());
    assertEquals("Aaa", job.getUpperBound().get());
    assertEquals(0, job.getOffset());
  }

  @Test
  public void testCustomConstructor() {
    ScientificNameRangeCrawlContext job;

    job = new ScientificNameRangeCrawlContext(0, null, null);
    assertFalse(job.getLowerBound().isPresent());
    assertFalse(job.getUpperBound().isPresent());
    assertEquals(0, job.getOffset());

    job = new ScientificNameRangeCrawlContext(0, "aaa", null);
    assertTrue(job.getLowerBound().isPresent());
    assertEquals("aaa", job.getLowerBound().get());
    assertFalse(job.getUpperBound().isPresent());
    assertEquals(0, job.getOffset());

    job = new ScientificNameRangeCrawlContext(10, "aaa", "zzz");
    assertTrue(job.getLowerBound().isPresent());
    assertEquals("aaa", job.getLowerBound().get());
    assertTrue(job.getUpperBound().isPresent());
    assertEquals("zzz", job.getUpperBound().get());
    assertEquals(10, job.getOffset());
    assertThrows(IllegalArgumentException.class, () -> new ScientificNameRangeCrawlContext(0, "", null));
    assertThrows(IllegalArgumentException.class, () -> new ScientificNameRangeCrawlContext(-10, null, null));
    assertThrows(IllegalArgumentException.class, () -> new ScientificNameRangeCrawlContext(0, null, "abcd"));
  }

  @Test
  public void testSetAbsent() {
    ScientificNameRangeCrawlContext job = new ScientificNameRangeCrawlContext(0, "aaa", "zzz");

    job.setLowerBoundAbsent();
    assertFalse(job.getLowerBound().isPresent());
    assertTrue(job.getUpperBound().isPresent());

    job.setUpperBoundAbsent();
    assertFalse(job.getLowerBound().isPresent());
    assertFalse(job.getUpperBound().isPresent());
  }
}
