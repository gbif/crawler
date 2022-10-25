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
package org.gbif.crawler.xml.crawlserver.listener;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.crawler.CrawlConfiguration;
import org.gbif.crawler.CrawlContext;
import org.gbif.crawler.CrawlListener;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * This class uses SLF4J to log events that occur during crawling. It logs everything at INFO level
 * except errors which are at WARN. It also make two things available in the MDC: datasetKey and
 * attempt.
 */
public class LoggingCrawlListener<CTX extends CrawlContext>
    implements CrawlListener<CTX, String, List<Byte>> {

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
    LOG.info("Started crawl");
  }

  @Override
  public void error(Throwable e) {
    LOG.warn("Error during crawling: [{}], last request [{}]", lastContext, lastRequest, e);
  }

  @Override
  public void error(String msg) {
    LOG.warn(
        "Error during crawling: [{}], last request [{}], message [{}]",
        lastContext,
        lastRequest,
        msg);
  }

  @Override
  public void response(
      List<Byte> response,
      int retry,
      long duration,
      Optional<Integer> recordCount,
      Optional<Boolean> endOfRecords) {
    totalRecordCount += recordCount.orElse(0);
    LOG.info(
        "Got response for [{}], records [{}], endOfRecords [{}], retry [{}], took [{}s]",
        lastContext,
        recordCount,
        endOfRecords,
        retry,
        TimeUnit.MILLISECONDS.toSeconds(duration));
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
        "Finished crawling with a total of [{}] records, reason [{}], started at [{}], finished at [{}], took [{}] minutes",
        totalRecordCount,
        reason,
        startDate,
        finishDate,
        minutes);

    MDC.remove("datasetKey");
    MDC.remove("attempt");
  }

  @Override
  public void request(String req, int retry) {
    LOG.info("Requested page for [{}], retry [{}], request [{}]", lastContext, retry, req);
    lastRequest = req;
  }

  @Override
  public void progress(CrawlContext context) {
    lastRequest = null;
    lastContext = context;
    LOG.info("Now beginning to crawl [{}]", context);
  }
}
