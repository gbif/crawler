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
package org.gbif.crawler.protocol.digir;

import java.net.URI;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DigirCrawlConfigurationTest {

  @Test
  public void testJob() {
    URI targetUrl = URI.create("http://mockhost1.gbif.org/tapirlink/tapir.php/pontaurus");
    UUID uuid = UUID.randomUUID();
    String resourceCode = "ent";

    testFailure(null, 1, targetUrl, resourceCode, true, "datasetKey");
    testFailure(uuid, 0, targetUrl, resourceCode, true, "attempt");
    testFailure(uuid, -10, targetUrl, resourceCode, true, "attempt");
    testFailure(uuid, 1, null, resourceCode, true, "url");
    testFailure(uuid, 1, targetUrl, null, true, "resource");
    testFailure(uuid, 1, targetUrl, "", true, "resource");

    DigirCrawlConfiguration job =
        new DigirCrawlConfiguration(uuid, 1, targetUrl, resourceCode, true);

    assertEquals(resourceCode, job.getResourceCode());
    assertEquals(targetUrl, job.getUrl());
    assertEquals(1, job.getAttempt());
    assertEquals(uuid, job.getDatasetKey());
    assertTrue(job.isManis());
  }

  private void testFailure(
      UUID uuid, int attempt, URI url, String resourceCode, boolean manis, String expectedString) {
    Exception exception =
        assertThrows(Exception.class, () -> new DigirCrawlConfiguration(uuid, attempt, url, resourceCode, manis));
    assertTrue(exception.getMessage().contains(expectedString));
  }
}
