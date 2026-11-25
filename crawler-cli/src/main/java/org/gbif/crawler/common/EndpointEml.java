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
package org.gbif.crawler.common;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.vocabulary.MetadataType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.UUID;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.CrawlerCoordinatorServiceImpl.EML_URL_PROPERTY;

/**
 * Location of the EML document downloaded from a dataset's separate EML endpoint, kept next to the
 * data package archive of the same crawl attempt (e.g. {@code datasetKey.attempt.eml.xml}).
 *
 * <p>Only DwC-DP and COLDP crawls download it, see {@code
 * CrawlerCoordinatorServiceImpl#getMetadataEndpoint}.
 */
public final class EndpointEml {

  public static final String SUFFIX = ".eml.xml";
  public static final String FAILED_SUFFIX = ".eml.failed";

  private static final Logger LOG = LoggerFactory.getLogger(EndpointEml.class);

  private EndpointEml() {}

  public static File file(File datasetDirectory, UUID datasetKey, int attempt) {
    return new File(datasetDirectory, datasetKey + "." + attempt + SUFFIX);
  }

  /** Marker written when the EML endpoint could not be downloaded for this attempt. */
  public static File failedMarker(File datasetDirectory, UUID datasetKey, int attempt) {
    return new File(datasetDirectory, datasetKey + "." + attempt + FAILED_SUFFIX);
  }

  /**
   * True if the dataset has an EML endpoint but it could not be downloaded for this attempt. Metasync
   * then leaves the registry metadata unchanged instead of falling back to the archive's own
   * metadata: the archive document would be stored as the most recent one and stay preferred over
   * the EML until the EML itself changes.
   */
  public static boolean downloadFailed(File datasetDirectory, UUID datasetKey, int attempt) {
    return failedMarker(datasetDirectory, datasetKey, attempt).exists();
  }

  /** Returns the downloaded EML for this attempt, if there is a non-empty one. */
  public static Optional<File> find(File datasetDirectory, UUID datasetKey, int attempt) {
    File eml = file(datasetDirectory, datasetKey, attempt);
    return eml.isFile() && eml.length() > 0 ? Optional.of(eml) : Optional.empty();
  }

  /** Reads the archive's own metadata as JSON, sent to the registry alongside the EML. */
  @FunctionalInterface
  public interface ContentJsonReader {
    @Nullable
    String read(File archive) throws IOException;
  }

  /** Sends the archive's own metadata to the registry, as metasync does without an endpoint EML. */
  @FunctionalInterface
  public interface ArchiveMetadataSender {
    void send(File archive) throws IOException;
  }

  /**
   * Syncs metadata for a dataset with an EML endpoint when its data package archive was not
   * modified, the same way metasync does for a modified one: the downloaded endpoint EML if there
   * is one, otherwise (the archive is more recent than the EML) the archive's own metadata, and
   * nothing if the EML could not be downloaded. An EML-only change still reaches the registry, and
   * archive metadata missed earlier (e.g. the first crawl's EML download failed) is sent now.
   * Nothing else is triggered (no validation or ingestion). The registry ignores a document
   * identical to the one it already has.
   *
   * <p>Datasets without an EML endpoint are left alone. Never throws: failures are logged and the
   * crawl finishes as not modified.
   *
   * @param archive the unchanged archive
   */
  public static void syncForUnmodifiedArchive(
      @Nullable OkHttpRegistryMetadataClient registryClient,
      File datasetDirectory,
      CrawlJob crawlJob,
      File archive,
      ContentJsonReader contentJsonReader,
      ArchiveMetadataSender archiveMetadataSender) {
    UUID datasetKey = crawlJob.getDatasetKey();
    int attempt = crawlJob.getAttempt();
    if (registryClient == null
        || crawlJob.getProperty(EML_URL_PROPERTY) == null
        || downloadFailed(datasetDirectory, datasetKey, attempt)) {
      return;
    }
    try {
      boolean emlSent =
          find(datasetDirectory, datasetKey, attempt).isPresent()
              && insertIfPresent(
                  registryClient,
                  datasetDirectory,
                  datasetKey,
                  attempt,
                  contentJsonReader.read(archive));
      if (!emlSent) {
        archiveMetadataSender.send(archive);
      }
    } catch (Exception e) {
      LOG.warn(
          "Unable to sync metadata for unmodified archive of dataset [{}]",
          datasetKey,
          e);
    }
  }

  /**
   * Sends the downloaded endpoint EML for this attempt to the registry, if there is one.
   *
   * @param contentJson the data package's own metadata, sent alongside as today
   * @return true if the registry accepted it; false if there is none or it was rejected, in which
   *     case the caller should fall back to the archive's own metadata
   */
  public static boolean insertIfPresent(
      OkHttpRegistryMetadataClient registryClient,
      File datasetDirectory,
      UUID datasetKey,
      int attempt,
      String contentJson) {
    Optional<File> eml = find(datasetDirectory, datasetKey, attempt);
    if (!eml.isPresent()) {
      return false;
    }
    try {
      registryClient.insertMetadata(
          datasetKey, Files.readAllBytes(eml.get().toPath()), contentJson, MetadataType.EML);
      LOG.info("Forwarded EML endpoint metadata to registry for dataset [{}]", datasetKey);
      return true;
    } catch (IOException e) {
      LOG.warn(
          "Registry did not accept EML endpoint metadata for dataset [{}], using archive metadata instead",
          datasetKey,
          e);
      return false;
    }
  }
}
