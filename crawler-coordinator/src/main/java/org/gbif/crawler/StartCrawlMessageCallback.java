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
package org.gbif.crawler;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.StartCrawlMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a callback class to be used with the GBIF Postal Service. It listens for {@link
 * StartCrawlMessage}s and passes them on to a {@link CrawlerCoordinatorService}.
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
      if (message.getPriority() != null) {
        service.initiateCrawl(
            message.getDatasetUuid(), message.getPriority(), message.getPlatform());
      } else {
        service.initiateCrawl(message.getDatasetUuid(), message.getPlatform());
      }
    } catch (AlreadyCrawlingException e) {
      LOG.warn(e.getMessage());
    } catch (Exception e) {
      LOG.error("Caught exception while trying to enqueue crawl [{}]", message.getDatasetUuid(), e);
    }
  }
}
