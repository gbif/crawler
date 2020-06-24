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
package org.gbif.crawler.protocol.biocase;

import java.net.URI;
import java.util.UUID;

import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class BiocaseCrawlConfigurationTest {

  @Test
  public void testJob() {
    URI targetUrl = URI.create("http://mockhost1.gbif.org/tapirlink/tapir.php/pontaurus");
    UUID uuid = UUID.randomUUID();
    String contentNamespace = "http://www.tdwg.org/schemas/abcd/1.2";
    String datasetTitle = "Pontaurus";

    testFailure(null, 1, targetUrl, contentNamespace, datasetTitle, "datasetKey");
    testFailure(uuid, 0, targetUrl, contentNamespace, datasetTitle, "attempt");
    testFailure(uuid, -10, targetUrl, contentNamespace, datasetTitle, "attempt");
    testFailure(uuid, 1, null, contentNamespace, datasetTitle, "url");
    testFailure(uuid, 1, targetUrl, null, datasetTitle, "Namespace");
    testFailure(uuid, 1, targetUrl, "", datasetTitle, "Namespace");
    testFailure(uuid, 1, targetUrl, "foobar", datasetTitle, "Namespace");

    BiocaseCrawlConfiguration job =
        new BiocaseCrawlConfiguration(uuid, 1, targetUrl, contentNamespace, datasetTitle);

    assertThat(job.getContentNamespace()).isEqualTo(contentNamespace);
    assertThat(job.getUrl()).isEqualTo(targetUrl);
    assertThat(job.getAttempt()).isEqualTo(1);
    assertThat(job.getDatasetKey()).isEqualTo(uuid);
    assertThat(job.getDatasetTitle()).isEqualTo(datasetTitle);
  }

  private void testFailure(
      UUID uuid,
      int attempt,
      URI url,
      String contentNamespace,
      String datasetTitle,
      String expectedString) {
    try {
      new BiocaseCrawlConfiguration(uuid, attempt, url, contentNamespace, datasetTitle);
      fail();
    } catch (Exception ex) {
      assertThat(ex).hasMessageContaining(expectedString);
    }
  }
}
