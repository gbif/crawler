package org.gbif.crawler.xml.crawlserver.listener;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.crawler.CrawlConfiguration;
import org.gbif.crawler.CrawlContext;
import org.gbif.crawler.CrawlListener;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * This class uses SLF4J to log events that occur during crawling. It logs everything at INFO level except errors which
 * are at WARN. It also make two things available in the MDC: datasetKey and attempt.
 */
public class LoggingCrawlListener<CTX extends CrawlContext> implements CrawlListener<CTX, String, List<Byte>> {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingCrawlListener.class);

  private CrawlContext lastContext;

  private String lastRequest;

  private int totalRecordCount;

  private Date startDate;

  public LoggingCrawlListener(CrawlConfiguration configuration) {
    MDC.put("datasetKey", configuration.getDatasetKey().toString());
    MDC.put("attempt", String.valueOf(configuration.getAttempt()));
  }

  @Override
  public void startCrawl() {
    this.startDate = new Date();
    LOG.info("started crawl");
  }

  @Override
  public void error(Throwable e) {
    LOG.warn("error during crawling: [{}], last request [{}]", lastContext, lastRequest, e);
  }

  @Override
  public void error(String msg) {
    LOG.warn("error during crawling: [{}], last request [{}], message [{}]", lastContext, lastRequest, msg);
  }

  @Override
  public void response(List<Byte> response, int retry, long duration, Optional<Integer> recordCount,
    Optional<Boolean> endOfRecords) {
    totalRecordCount += recordCount.or(0);
    LOG.info("got response for [{}], records [{}], endOfRecords [{}], retry [{}], took [{}s]", lastContext, recordCount,
      endOfRecords, retry, TimeUnit.MILLISECONDS.toSeconds(duration));
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
    Date finishDate = new Date();
    long minutes = (finishDate.getTime() - startDate.getTime()) / (60 * 1000);
    LOG.info(
      "finished crawling with a total of [{}] records, reason [{}], started at [{}], finished at [{}], took [{}] minutes",
      totalRecordCount, reason, startDate, finishDate, minutes);

    MDC.remove("datasetKey");
    MDC.remove("attempt");
  }

  @Override
  public void request(String req, int retry) {
    LOG.info("requested page for [{}], retry [{}], request [{}]", lastContext, retry, req);
    lastRequest = req;
  }

  @Override
  public void progress(CrawlContext context) {
    lastRequest = null;
    lastContext = context;
    LOG.info("now beginning to crawl [{}]", context);
  }

}
