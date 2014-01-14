package org.gbif.crawler;

import org.gbif.api.exception.ServiceUnavailableException;

import java.util.UUID;

/**
 * The public interface of the Crawler Coordinator. This allows to schedule a crawl.
 */
public interface CrawlerCoordinatorService {

  /**
   * Initiates a crawl of an existing dataset at a given priority.
   *
   * @param datasetKey of the dataset to crawl
   * @param priority    of this crawl. Lower numbers mean higher priorities. This priority can be chosen arbitrarily.
   *
   * @throws ServiceUnavailableException if there are any problems communicating with the Registry or ZooKeeper.
   *                                     ZooKeeper will already have been retried.
   * @throws IllegalArgumentException    if the dataset doesn't exist, we don't support its protocol, it isn't eligible
   *                                     for crawling
   * @throws AlreadyCrawlingException    if the dataset is already being crawled
   */
  void initiateCrawl(UUID datasetKey, int priority);

  /**
   * Initiates a crawl of an existing dataset without any explicit priority. Implementations are free to chose a
   * default priority.
   *
   * @param datasetKey of the dataset to crawl
   *
   * @throws ServiceUnavailableException if there are any problems communicating with the Registry or ZooKeeper.
   *                                     ZooKeeper will already have been retried.
   * @throws IllegalArgumentException    if the dataset doesn't exist, we don't support its protocol, it isn't eligible
   *                                     for crawling
   * @throws AlreadyCrawlingException    if the dataset is already being crawled
   */
  void initiateCrawl(UUID datasetKey);

}
