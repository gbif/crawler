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

import org.gbif.api.vocabulary.MetadataType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Location of the EML document downloaded from a dataset's separate EML endpoint, kept next to the
 * data package archive of the same crawl attempt (e.g. {@code datasetKey.attempt.eml.xml}).
 *
 * <p>Only DwC-DP and COLDP crawls download it, see {@code
 * CrawlerCoordinatorServiceImpl#getMetadataEndpoint}.
 */
public final class EndpointEml {

  public static final String SUFFIX = ".eml.xml";

  private static final Logger LOG = LoggerFactory.getLogger(EndpointEml.class);

  private EndpointEml() {}

  public static File file(File datasetDirectory, UUID datasetKey, int attempt) {
    return new File(datasetDirectory, datasetKey + "." + attempt + SUFFIX);
  }

  /** Returns the downloaded EML for this attempt, if there is a non-empty one. */
  public static Optional<File> find(File datasetDirectory, UUID datasetKey, int attempt) {
    File eml = file(datasetDirectory, datasetKey, attempt);
    return eml.isFile() && eml.length() > 0 ? Optional.of(eml) : Optional.empty();
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
