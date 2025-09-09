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
package org.gbif.crawler.dwcdp.downloader;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DatasetBasedMessage;
import org.gbif.common.messaging.api.messages.DwcDpDownloadFinishedMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.common.DownloadCrawlConsumer;

import java.io.File;
import java.util.Date;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;

import org.gbif.crawler.dwcdp.DwcDpConfiguration;

/**
 * Consumer of the crawler queue that runs the actual DwcDp archive download and emits a
 * DwcDpDownloadFinishedMessage when done.
 */
public class DwcDpCrawlConsumer extends DownloadCrawlConsumer {

  public DwcDpCrawlConsumer(
      CuratorFramework curator,
      MessagePublisher publisher,
      File archiveRepository,
      int httpTimeout) {
    super(curator, publisher, archiveRepository, httpTimeout);
  }

  @Override
  protected DatasetBasedMessage createFinishedMessage(CrawlJob crawlJob) {
    return new DwcDpDownloadFinishedMessage(
        crawlJob.getDatasetKey(),
        crawlJob.getTargetUrl(),
        crawlJob.getAttempt(),
        new Date(),
        true,
        crawlJob.getEndpointType(),
        Platform.parseOrDefault(crawlJob.getProperty("platform"), Platform.ALL));
  }

  @Override
  protected String getSuffix() {
    return DwcDpConfiguration.DWC_DP_SUFFIX;
  }

  @Override
  protected File getArchiveDirectory(File archiveRepository, UUID datasetKey) {
    return new File(archiveRepository, datasetKey.toString());
  }
}
