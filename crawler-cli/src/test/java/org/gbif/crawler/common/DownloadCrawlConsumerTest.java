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
package org.gbif.crawler.common;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.DatasetBasedMessage;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("Manual tests check download behaviour")
public class DownloadCrawlConsumerTest {

  // please adapt to personal needs when running the tests manually!
  static final File DWCA_REPO = new File("/tmp/dwcacrawlconsumertest");

  static {
    try {
      Files.createDirectory(DWCA_REPO.toPath());
    } catch (IOException e) {
    }
  }

  /**
   * Test can be run repeatedly, observe the download is hard-linked using the crawl attempt id (ls
   * -li), and old downloads aren't lost when the download is updated.
   */
  @Test
  public void testDownload() throws Exception {

    final UUID datasetKey = UUID.randomUUID().fromString("136c1f7b-0a39-4374-9362-f2bd467a2a93");

    int attempt = 1;
    // Record latest attempt number in a file.
    Path attemptFile = new File(DWCA_REPO, datasetKey + ".crawlId").toPath();
    try {
      attempt = Integer.valueOf(Files.readAllLines(attemptFile).get(0)) + 1;
    } catch (Exception e) {
    }
    try {
      Files.write(attemptFile, Arrays.asList("" + attempt));
    } catch (Exception e) {
    }

    DownloadCrawlConsumer cc =
        new DownloadCrawlConsumer(null, null, DWCA_REPO, 10 * 60 * 1000) {
          @Override
          protected DatasetBasedMessage createFinishedMessage(CrawlJob crawlJob) {
            return null;
          }

          @Override
          protected String getSuffix() {
            return ".suffix";
          }

          @Override
          protected File getArchiveDirectory(File archiveRepository, UUID datasetKey) {
            return archiveRepository;
          }
        };
    CrawlJob test =
        new CrawlJob(
            datasetKey,
            attempt,
            EndpointType.DWC_ARCHIVE,
            URI.create("https://hosted-datasets.gbif.org/datasets/eunis.zip"));
    cc.crawl(datasetKey, test);
  }
}
