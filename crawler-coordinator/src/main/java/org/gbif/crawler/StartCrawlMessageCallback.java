package org.gbif.crawler;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.StartCrawlMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a callback class to be used with the GBIF Postal Service. It listens for {@link StartCrawlMessage}s and
 * passes them on to a {@link CrawlerCoordinatorService}.
 */
public class StartCrawlMessageCallback extends AbstractMessageCallback<StartCrawlMessage> {

  private final CrawlerCoordinatorService service;

  private static final Logger LOG = LoggerFactory.getLogger(StartCrawlMessageCallback.class);

  public StartCrawlMessageCallback(CrawlerCoordinatorService service) {
    this.service = service;
  }

  @Override
  public void handleMessage(StartCrawlMessage message) {
    LOG.debug("Trying to enqueue crawl for dataset [{}]", message.getDatasetUuid());
    try {
      if (message.getPriority().isPresent()) {
        service.initiateCrawl(message.getDatasetUuid(), message.getPriority().get());
      } else {
        service.initiateCrawl(message.getDatasetUuid());
      }
    } catch (AlreadyCrawlingException e) {
      LOG.warn(e.getMessage());
    } catch (Exception e) {
      LOG.error("Caught exception while trying to enqueue crawl [{}]", message.getDatasetUuid(), e);
    }
  }

}
