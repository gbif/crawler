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
package org.gbif.crawler.protocol.tapir;

import java.net.URI;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TapirCrawlConfigurationTest {

  @Test
  public void testJob() {
    URI targetUrl = URI.create("http://mockhost1.gbif.org/tapirlink/tapir.php/pontaurus");
    UUID uuid = UUID.randomUUID();
    String contentNamespace = "http://rs.tdwg.org/dwc/dwcore/";

    testFailure(null, 1, targetUrl, contentNamespace, "datasetKey");
    testFailure(uuid, 0, targetUrl, contentNamespace, "attempt");
    testFailure(uuid, -10, targetUrl, contentNamespace, "attempt");
    testFailure(uuid, 1, null, contentNamespace, "url");
    testFailure(uuid, 1, targetUrl, null, "Namespace");
    testFailure(uuid, 1, targetUrl, "foobar", "support");

    TapirCrawlConfiguration job = new TapirCrawlConfiguration(uuid, 1, targetUrl, contentNamespace);

    assertEquals(contentNamespace, job.getContentNamespace());
    assertEquals(targetUrl, job.getUrl());
    assertEquals(1, job.getAttempt());
    assertEquals(uuid, job.getDatasetKey());
  }

  private void testFailure(
      UUID uuid, int attempt, URI url, String contentNamespace, String expectedString) {
    Exception exception =
        assertThrows(Exception.class, () -> new TapirCrawlConfiguration(uuid, attempt, url, contentNamespace));
    assertTrue(exception.getMessage().contains(expectedString));
  }
}
