package org.gbif.crawler.xml.crawlserver.listener;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.CrawlErrorMessage;
import org.gbif.common.messaging.api.messages.CrawlFinishedMessage;
import org.gbif.common.messaging.api.messages.CrawlRequestMessage;
import org.gbif.common.messaging.api.messages.CrawlResponseMessage;
import org.gbif.common.messaging.api.messages.CrawlStartedMessage;
import org.gbif.crawler.CrawlConfiguration;
import org.gbif.crawler.CrawlContext;
import org.gbif.crawler.CrawlListener;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.primitives.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class MessagingCrawlListener<CTX extends CrawlContext> implements CrawlListener<CTX, String, List<Byte>> {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingCrawlListener.class);

  private final MessagePublisher publisher;

  private final CrawlConfiguration configuration;

  private CrawlContext lastContext;

  private int totalRecordCount;

  private int retry;

  /**
   * This is the last duration it took to get a response in milliseconds that we received
   */
  private long duration;

  public MessagingCrawlListener(MessagePublisher publisher, CrawlConfiguration configuration) {
    this.publisher = checkNotNull(publisher);
    this.configuration = checkNotNull(configuration);
  }

  @Override
  public void startCrawl() {
    Message msg =
      new CrawlStartedMessage(configuration.getDatasetKey(), configuration.getAttempt(), configuration.getUrl(),
        configuration.toString());
    sendMessageSilently(msg);
  }

  @Override
  public void request(String req, int retry) {
    this.retry = retry;
    Message msg = new CrawlRequestMessage(configuration.getDatasetKey(), configuration.getAttempt(), retry,
      req.getBytes(Charsets.UTF_8), lastContext.toString());
    sendMessageSilently(msg);
  }

  @Override
  public void response(List<Byte> response, int retry, long duration, Optional<Integer> recordCount,
    Optional<Boolean> endOfRecords) {
    if (recordCount.isPresent()) {
      totalRecordCount += recordCount.get();
    }
    this.retry = retry;
    this.duration = duration;
    Message msg = new CrawlResponseMessage(configuration.getDatasetKey(), configuration.getAttempt(), retry,
      Bytes.toArray(response), duration, recordCount, lastContext.toString());
    sendMessageSilently(msg);
  }

  @Override
  public void error(Throwable e) {
    // TODO: Error type
    Message msg =
      new CrawlErrorMessage(configuration.getDatasetKey(), configuration.getAttempt(), retry, lastContext.getOffset(),
        duration, lastContext.toString(), CrawlErrorMessage.ErrorType.UNKNOWN, e);
    sendMessageSilently(msg);
  }

  @Override
  public void error(String msg) {
    // TODO: Error type
    Message message =
      new CrawlErrorMessage(configuration.getDatasetKey(), configuration.getAttempt(), retry, lastContext.getOffset(),
        duration, lastContext.toString(), CrawlErrorMessage.ErrorType.UNKNOWN, null);
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
    finishCrawl(FinishReason.NORMAL);
  }

  private void finishCrawl(FinishReason reason) {
    Message msg =
      new CrawlFinishedMessage(configuration.getDatasetKey(), configuration.getAttempt(), totalRecordCount, reason);
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
