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
package org.gbif.crawler.coldp.downloader;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.ColDpDownloadFinishedMessage;
import org.gbif.common.messaging.api.messages.DatasetBasedMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.coldp.ColDpConfiguration;
import org.gbif.crawler.coldp.metasync.ColDpMetadataDocumentConverter;
import org.gbif.crawler.coldp.metasync.ColDpMetasyncCallback;
import org.gbif.crawler.common.DownloadCrawlConsumer;
import org.gbif.crawler.common.EndpointEml;
import org.gbif.crawler.common.OkHttpRegistryMetadataClient;

import java.io.File;
import java.util.Date;
import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.curator.framework.CuratorFramework;

public class ColDpCrawlConsumer extends DownloadCrawlConsumer {

  @Nullable private final OkHttpRegistryMetadataClient registryClient;
  private final ColDpMetadataDocumentConverter converter = new ColDpMetadataDocumentConverter();

  public ColDpCrawlConsumer(
      CuratorFramework curator,
      MessagePublisher publisher,
      File archiveRepository,
      int httpTimeout) {
    this(curator, publisher, archiveRepository, httpTimeout, null);
  }

  /**
   * @param registryClient used to sync the EML endpoint when the archive was not modified; without
   *     it an EML-only change is only picked up with the next modified archive
   */
  public ColDpCrawlConsumer(
      CuratorFramework curator,
      MessagePublisher publisher,
      File archiveRepository,
      int httpTimeout,
      @Nullable OkHttpRegistryMetadataClient registryClient) {
    super(curator, publisher, archiveRepository, httpTimeout);
    this.registryClient = registryClient;
  }

  @Override
  protected void afterNotModified(
      UUID datasetKey, CrawlJob crawlJob, File datasetDirectory, File localFile) {
    EndpointEml.syncForUnmodifiedArchive(
        registryClient,
        datasetDirectory,
        crawlJob,
        localFile,
        archive -> converter.extractDocuments(archive).getFormatContentJson(),
        archive ->
            ColDpMetasyncCallback.insertArchiveMetadata(
                registryClient, datasetKey, converter.extractDocuments(archive)));
  }

  @Override
  protected DatasetBasedMessage createFinishedMessage(CrawlJob crawlJob) {
    return new ColDpDownloadFinishedMessage(
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
    return ColDpConfiguration.COLDP_SUFFIX;
  }

  @Override
  protected File getArchiveDirectory(File archiveRepository, UUID datasetKey) {
    return new File(archiveRepository, datasetKey.toString());
  }
}
