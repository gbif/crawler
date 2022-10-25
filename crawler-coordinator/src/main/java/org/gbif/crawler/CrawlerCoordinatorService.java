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
package org.gbif.crawler;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.common.messaging.api.messages.Platform;

import java.util.UUID;

/** The public interface of the Crawler Coordinator. This allows to schedule a crawl. */
public interface CrawlerCoordinatorService {

  /**
   * Initiates a crawl of an existing dataset at a given priority.
   *
   * @param datasetKey of the dataset to crawl
   * @param priority of this crawl. Lower numbers mean higher priorities. This priority can be
   *     chosen arbitrarily.
   * @param platform indexing platform that performs the crawl
   * @throws ServiceUnavailableException if there are any problems communicating with the Registry
   *     or ZooKeeper. ZooKeeper will already have been retried.
   * @throws IllegalArgumentException if the dataset doesn't exist, we don't support its protocol,
   *     it isn't eligible for crawling
   * @throws AlreadyCrawlingException if the dataset is already being crawled
   */
  void initiateCrawl(UUID datasetKey, int priority, Platform platform);

  /**
   * Initiates a crawl of an existing dataset without any explicit priority. Implementations are
   * free to chose a default priority.
   *
   * @param datasetKey of the dataset to crawl
   * @param platform indexing platform that performs the crawl
   * @throws ServiceUnavailableException if there are any problems communicating with the Registry
   *     or ZooKeeper. ZooKeeper will already have been retried.
   * @throws IllegalArgumentException if the dataset doesn't exist, we don't support its protocol,
   *     it isn't eligible for crawling
   * @throws AlreadyCrawlingException if the dataset is already being crawled
   */
  void initiateCrawl(UUID datasetKey, Platform platform);
}
