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
package org.gbif.crawler.xml.crawlserver.listener;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.*;
import org.gbif.crawler.CrawlConfiguration;
import org.gbif.crawler.CrawlContext;
import org.gbif.crawler.CrawlListener;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.primitives.Bytes;

import static com.google.common.base.Preconditions.checkNotNull;

public class MessagingCrawlListener<CTX extends CrawlContext>
    implements CrawlListener<CTX, String, List<Byte>> {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingCrawlListener.class);

  private final MessagePublisher publisher;

  private final CrawlConfiguration configuration;

  private final EndpointType endpointType;

  private final Platform platform;

  private CrawlContext lastContext;

  private int totalRecordCount;

  private int retry;

  /** This is the last duration it took to get a response in milliseconds that we received */
  private long duration;

  public MessagingCrawlListener(
      MessagePublisher publisher,
      CrawlConfiguration configuration,
      EndpointType endpointType,
      Platform platform) {
    this.publisher = checkNotNull(publisher);
    this.configuration = checkNotNull(configuration);
    this.endpointType = endpointType;
    this.platform = platform;
  }

  @Override
  public void startCrawl() {
    Message msg =
        new CrawlStartedMessage(
            configuration.getDatasetKey(),
            configuration.getAttempt(),
            configuration.getUrl(),
            configuration.toString());
    sendMessageSilently(msg);
  }

  @Override
  public void request(String req, int retry) {
    this.retry = retry;
    Message msg =
        new CrawlRequestMessage(
            configuration.getDatasetKey(),
            configuration.getAttempt(),
            retry,
            req.getBytes(Charsets.UTF_8),
            lastContext.toString());
    sendMessageSilently(msg);
  }

  @Override
  public void response(
      List<Byte> response,
      int retry,
      long duration,
      Optional<Integer> recordCount,
      Optional<Boolean> endOfRecords) {
    if (recordCount.isPresent()) {
      totalRecordCount += recordCount.get();
    }
    this.retry = retry;
    this.duration = duration;

    if (Platform.OCCURRENCE.equivalent(platform)) {
      Message msg =
          new CrawlResponseMessage(
              configuration.getDatasetKey(),
              configuration.getAttempt(),
              retry,
              Bytes.toArray(response),
              duration,
              recordCount,
              lastContext.toString(),
              platform);
      sendMessageSilently(msg);
    }
  }

  @Override
  public void error(Throwable e) {
    // TODO: Error type
    Message msg =
        new CrawlErrorMessage(
            configuration.getDatasetKey(),
            configuration.getAttempt(),
            retry,
            lastContext.getOffset(),
            duration,
            lastContext.toString(),
            CrawlErrorMessage.ErrorType.UNKNOWN,
            e);
    sendMessageSilently(msg);
  }

  @Override
  public void error(String msg) {
    // TODO: Error type
    Message message =
        new CrawlErrorMessage(
            configuration.getDatasetKey(),
            configuration.getAttempt(),
            retry,
            lastContext.getOffset(),
            duration,
            lastContext.toString(),
            CrawlErrorMessage.ErrorType.UNKNOWN,
            null);
    sendMessageSilently(message);
  }

  @Override
  public void finishCrawlNormally() {
    finishCrawl(FinishReason.NORMAL);
  }

  @Override
  public void finishCrawlOnUserRequest() {
    finishCrawl(FinishReason.USER_ABORT);
  }

  @Override
  public void finishCrawlAbnormally() {
    finishCrawl(FinishReason.ABORT);
  }

  private void finishCrawl(FinishReason reason) {
    Message msg =
        new CrawlFinishedMessage(
            configuration.getDatasetKey(),
            configuration.getAttempt(),
            totalRecordCount,
            reason,
            endpointType,
            platform);
    sendMessageSilently(msg);
  }

  @Override
  public void progress(CrawlContext context) {
    lastContext = context;
    retry = 1;
    duration = 0;
  }

  private void sendMessageSilently(Message msg) {
    try {
      publisher.send(msg, true);
    } catch (IOException e) {
      LOG.warn("Could not send message: [{}]", msg);
    }
  }
}
