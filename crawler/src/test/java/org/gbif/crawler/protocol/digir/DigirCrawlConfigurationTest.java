package org.gbif.crawler.protocol.digir;

import java.net.URI;
import java.util.UUID;

import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.fail;

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

    DigirCrawlConfiguration job = new DigirCrawlConfiguration(uuid, 1, targetUrl, resourceCode, true);

    assertThat(job.getResourceCode()).isEqualTo(resourceCode);
    assertThat(job.getUrl()).isEqualTo(targetUrl);
    assertThat(job.getAttempt()).isEqualTo(1);
    assertThat(job.getDatasetKey()).isEqualTo(uuid);
    assertThat(job.isManis()).isTrue();
  }

  private void testFailure(UUID uuid, int attempt, URI url, String resourceCode, boolean manis,
    String expectedString) {
    try {
      new DigirCrawlConfiguration(uuid, attempt, url, resourceCode, manis);
      fail();
    } catch (Exception ex) {
      assertThat(ex).hasMessageContaining(expectedString);
    }
  }

}
