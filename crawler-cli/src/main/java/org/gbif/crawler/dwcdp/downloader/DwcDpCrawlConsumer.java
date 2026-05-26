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
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DatasetBasedMessage;
import org.gbif.common.messaging.api.messages.DwcDpDownloadFinishedMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.CrawlerCoordinatorServiceImpl;
import org.gbif.crawler.common.DownloadCrawlConsumer;
import org.gbif.crawler.dwcdp.DwcDpConfiguration;
import org.gbif.utils.file.CompressionUtil;
import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;

/**
 * Consumer of the crawler queue that runs the actual DwcDp archive download and emits a
 * DwcDpDownloadFinishedMessage when done.
 */
public class DwcDpCrawlConsumer extends DownloadCrawlConsumer {

  private static final String DATAPACKAGE_JSON = "datapackage.json";

  private final DatasetService datasetService;
  private final File unpackedDpRepository;
  public DwcDpCrawlConsumer(
      CuratorFramework curator,
      MessagePublisher publisher,
      File archiveRepository,
      File unpackedDpRepository,
      int httpTimeout,
      DatasetService datasetService) {
    super(curator, publisher, archiveRepository, httpTimeout);
    this.unpackedDpRepository = unpackedDpRepository;
    if (!unpackedDpRepository.exists() || !unpackedDpRepository.isDirectory()) {
      throw new IllegalArgumentException(
          "Unpacked DP repository needs to be an existing directory: "
              + unpackedDpRepository.getAbsolutePath());
    }
    if (!unpackedDpRepository.canWrite()) {
      throw new IllegalArgumentException(
          "Unpacked DP repository directory not writable: "
              + unpackedDpRepository.getAbsolutePath());
    }
    this.datasetService = datasetService;
  }

  @Override
  protected DatasetBasedMessage createFinishedMessage(CrawlJob crawlJob) {
    Dataset dataset = datasetService.get(crawlJob.getDatasetKey());
    Optional<Endpoint> endpoint = CrawlerCoordinatorServiceImpl.getEndpointToCrawl(dataset);
    return new DwcDpDownloadFinishedMessage(
        crawlJob.getDatasetKey(),
        crawlJob.getTargetUrl(),
        crawlJob.getAttempt(),
        new Date(),
        true,
        crawlJob.getEndpointType(),
        endpoint.map(Endpoint::getKey).orElse(null),
        Platform.parseOrDefault(crawlJob.getProperty("platform"), Platform.ALL));
  }

  @Override
  protected void afterSuccessfulDownload(UUID datasetKey, CrawlJob crawlJob, File localFile)
      throws IOException {
    File targetDirectory = new File(unpackedDpRepository, datasetKey.toString());
    File tempDirectory = new File(unpackedDpRepository, datasetKey + ".tmp");
    File backupDirectory = new File(unpackedDpRepository, datasetKey + ".bak");

    recreateDirectory(tempDirectory);
    try {
      CompressionUtil.decompressFile(tempDirectory, localFile, true);
    } catch (RuntimeException e) {
      throw new IOException("Unable to unpack DwcDP archive for dataset " + datasetKey, e);
    }
    validateUnpackedDp(datasetKey, tempDirectory);
    promoteTempDirectory(datasetKey, tempDirectory, targetDirectory, backupDirectory);
  }

  private static void recreateDirectory(File directory) throws IOException {
    if (directory.exists()) {
      FileUtils.deleteDirectoryRecursively(directory);
    }
    if (!directory.mkdirs() && !directory.isDirectory()) {
      throw new IOException("Unable to create directory " + directory);
    }
  }

  private static void validateUnpackedDp(UUID datasetKey, File unpackedDirectory) throws IOException {
    File datapackage = new File(unpackedDirectory, DATAPACKAGE_JSON);
    if (!datapackage.isFile()) {
      throw new IOException(
          "Unpacked DwcDP archive for dataset "
              + datasetKey
              + " does not contain "
              + DATAPACKAGE_JSON);
    }
  }

  private static void promoteTempDirectory(
      UUID datasetKey, File tempDirectory, File targetDirectory, File backupDirectory) throws IOException {
    if (backupDirectory.exists()) {
      FileUtils.deleteDirectoryRecursively(backupDirectory);
    }

    boolean movedCurrentToBackup = false;
    try {
      if (targetDirectory.exists()) {
        Files.move(targetDirectory.toPath(), backupDirectory.toPath());
        movedCurrentToBackup = true;
      }
      Files.move(tempDirectory.toPath(), targetDirectory.toPath());
      if (movedCurrentToBackup && backupDirectory.exists()) {
        FileUtils.deleteDirectoryRecursively(backupDirectory);
      }
    } catch (IOException e) {
      if (tempDirectory.exists()) {
        FileUtils.deleteDirectoryRecursively(tempDirectory);
      }
      if (movedCurrentToBackup && backupDirectory.exists() && !targetDirectory.exists()) {
        try {
          Files.move(backupDirectory.toPath(), targetDirectory.toPath());
        } catch (IOException restoreException) {
          e.addSuppressed(restoreException);
        }
      }
      throw new IOException(
          "Unable to promote unpacked DwcDP directory for dataset " + datasetKey, e);
    }
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
