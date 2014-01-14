/*
 * Copyright 2012 Global Biodiversity Information Facility (GBIF)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.constants;

import java.util.UUID;
import javax.annotation.Nullable;

import com.google.common.base.Joiner;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Constant class for storing the Zookeeper node paths.
 */
public class CrawlerNodePaths {

  public static final String SUCCESSFUL = "successful";
  public static final String ERROR = "error";
  public static final String CRAWL_INFO = "crawls";
  public static final String QUEUED_CRAWLS = "queuedCrawls";
  public static final String RUNNING_CRAWLS = "runningCrawls";
  public static final String LOCKED_URLS = "lockedUrls";
  public static final String XML_CRAWL = "xml";
  public static final String DWCA_CRAWL = "dwca";
  public static final String STARTED_CRAWLING = "startedCrawling";
  public static final String FINISHED_CRAWLING = "finishedCrawling";
  public static final String FINISHED_REASON = "finishedReason";
  public static final String CRAWL_CONTEXT = "crawlContext";
  public static final String DECLARED_COUNT = "declaredCount";
  public static final String PAGES_CRAWLED = "pagesCrawled";
  public static final String PAGES_FRAGMENTED_SUCCESSFUL = "pagesFragmented/" + SUCCESSFUL;
  public static final String PAGES_FRAGMENTED_ERROR = "pagesFragmented/" + ERROR;
  public static final String FRAGMENTS_EMITTED = "fragmentsEmitted";
  public static final String FRAGMENTS_RECEIVED = "fragmentsReceived";
  public static final String RAW_OCCURRENCES_PERSISTED_NEW = "rawOccurrencesPersisted/new";
  public static final String RAW_OCCURRENCES_PERSISTED_UPDATED = "rawOccurrencesPersisted/updated";
  public static final String RAW_OCCURRENCES_PERSISTED_UNCHANGED = "rawOccurrencesPersisted/unchanged";
  public static final String RAW_OCCURRENCES_PERSISTED_ERROR = "rawOccurrencesPersisted/" + ERROR;
  public static final String FRAGMENTS_PROCESSED = "fragmentsProcessed";
  public static final String VERBATIM_OCCURRENCES_PERSISTED_SUCCESSFUL = "verbatimOccurrencesPersisted/" + SUCCESSFUL;
  public static final String VERBATIM_OCCURRENCES_PERSISTED_ERROR = "verbatimOccurrencesPersisted/" + ERROR;
  public static final String INTERPRETED_OCCURRENCES_PERSISTED_SUCCESSFUL =
    "interpretedOccurrencesPersisted/" + SUCCESSFUL;
  public static final String INTERPRETED_OCCURRENCES_PERSISTED_ERROR = "interpretedOccurrencesPersisted/" + ERROR;
  private static final Joiner JOINER = Joiner.on('/').skipNulls();

  /**
   * Helper method to retrieve the path in ZooKeeper where all information about a specific dataset can be found.
   *
   * @param uuid to retrieve path for
   *
   * @return crawl info path for dataset
   */
  public static String getCrawlInfoPath(UUID uuid) {
    return getCrawlInfoPath(uuid, null);
  }

  /**
   * Helper method to retrieve a path under the {@link #CRAWL_INFO} node.
   *
   * @param uuid of the dataset to get the path for
   * @param path if null we retrieve the path of the {@link #CRAWL_INFO} node itself otherwise we append this path
   */
  public static String getCrawlInfoPath(UUID uuid, @Nullable String path) {
    checkNotNull(uuid, "uuid can't be null");
    String resultPath = CRAWL_INFO + "/" + uuid.toString();
    if (path != null) {
      resultPath += "/" + path;
    }

    return resultPath;
  }

  /**
   * Builds a "/" separated path out of path elements. The returned string will not have a "/" as the first and last
   * character.
   *
   * @param paths to concatenate
   */
  public static String buildPath(String... paths) {
    return JOINER.join(paths);
  }

  private CrawlerNodePaths() {
  }

}
