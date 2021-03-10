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

import org.gbif.crawler.exception.FatalCrawlException;
import org.gbif.crawler.exception.ProtocolException;
import org.gbif.crawler.exception.TransportException;
import org.gbif.crawler.protocol.biocase.BiocaseCrawlConfiguration;
import org.gbif.crawler.protocol.biocase.BiocaseScientificNameRangeRequestHandler;
import org.gbif.crawler.retry.LimitedRetryPolicy;
import org.gbif.crawler.strategy.ScientificNameRangeCrawlContext;
import org.gbif.crawler.strategy.ScientificNameRangeStrategy;
import org.gbif.wrangler.lock.NoLockFactory;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.http.HttpResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CrawlerTest {

  // * Test a normal always successful crawl from beginning to end
  // * Test combinations of Protocol and Transport exceptions and their handling
  // * Test all 16 possible response conditions

  /**
   * This Mockito Answer returns a different optional long every time it is called. It can be
   * initialized by providing a Iterator of Optional longs. Every time a present value is
   * encountered a random long is returned. So the passed in sequence serves as a pattern which is
   * cycled over.
   */
  @SuppressWarnings("unchecked")
  private static class CyclingHashAnswer implements Answer<Optional<Long>> {

    private final Random rnd = new Random();
    private final Iterator<Optional<Long>> cycle;

    /** The default is a cycle of present-absent-absent. */
    private CyclingHashAnswer() {
      cycle =
          Iterables.cycle(Optional.of(1L), Optional.<Long>absent(), Optional.<Long>absent())
              .iterator();
    }

    private CyclingHashAnswer(Optional<Long>... values) {
      cycle = Iterables.cycle(values).iterator();
    }

    @Override
    public Optional<Long> answer(InvocationOnMock invocation) throws Throwable {
      Optional<Long> next = cycle.next();
      if (next.isPresent()) {
        return Optional.of(rnd.nextLong());
      }
      return next;
    }
  }

  private static final UUID DATASET_UUID = UUID.randomUUID();

  // We have 26 * 26 = 676 + 2 (null->aaa, zza->null) + 1 (empty) = 679 different "bounds" which
  // makes for 678 requests in total
  private static final int NUM_RANGES = 26 * 26 + 2 + 1 - 1;
  private ScientificNameRangeCrawlContext context;
  private ScientificNameRangeStrategy strategy;
  private LimitedRetryPolicy retryPolicy;
  private final BiocaseCrawlConfiguration job =
      new BiocaseCrawlConfiguration(
          DATASET_UUID,
          5,
          URI.create("http://gbif.org"),
          "http://www.tdwg.org/schemas/abcd/2.06",
          "foo");

  private final BiocaseScientificNameRangeRequestHandler requestHandler =
      new BiocaseScientificNameRangeRequestHandler(job);

  @Mock private ResponseHandler<HttpResponse, List<Byte>> responseHandler;

  @Mock private CrawlClient<String, HttpResponse> client;

  @Mock private CrawlListener<ScientificNameRangeCrawlContext, String, List<Byte>> crawlListener;

  private Crawler<ScientificNameRangeCrawlContext, String, HttpResponse, List<Byte>> crawler;
  @BeforeEach
  public void setUp() {
    context = new ScientificNameRangeCrawlContext();
    strategy = new ScientificNameRangeStrategy(context);
    retryPolicy = new LimitedRetryPolicy(1, 1, 1, 1);
    crawler =
        Crawler.newInstance(
            strategy,
            requestHandler,
            responseHandler,
            client,
            retryPolicy,
            NoLockFactory.getLock());
    crawler.addListener(crawlListener);
    when(responseHandler.isValidState()).thenReturn(true);
  }

  /** Necessary to avoid running out of RAM. */
  @AfterEach
  public void tearDown() {
    Mockito.reset(responseHandler, client, crawlListener);
  }

  @Test
  public void testAbortFatalCrawlException() throws Exception {
    when(client.execute(anyString(), eq(responseHandler)))
        .thenThrow(new FatalCrawlException("foo"));
    crawler.crawl();
    verify(client, times(1)).execute(anyString(), eq(responseHandler));
  }

  @Test
  public void testRetryAbortProtocolException() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.absent());
    when(responseHandler.getRecordCount()).thenReturn(Optional.absent());
    when(responseHandler.getContentHash()).thenReturn(Optional.absent());
    when(client.execute(anyString(), eq(responseHandler))).thenThrow(new ProtocolException("foo"));
    crawler.crawl();
    assertTrue(retryPolicy.abortCrawl());
    verify(client, times(2)).execute(anyString(), eq(responseHandler));
  }

  @Test
  public void testRetryAbortTransportException() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.absent());
    when(responseHandler.getRecordCount()).thenReturn(Optional.absent());
    when(responseHandler.getContentHash()).thenReturn(Optional.absent());
    when(client.execute(anyString(), eq(responseHandler))).thenThrow(new TransportException("foo"));
    crawler.crawl();
    assertTrue(retryPolicy.abortCrawl());
    verify(client, times(2)).execute(anyString(), eq(responseHandler));
  }

  /**
   * unknown end of records known record count got content in first request, then nothing on next
   * page
   *
   * <p>relying on speculative requests to skip to next range
   */
  @Test
  public void testScenario11And12And14() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.of(false));
    when(responseHandler.getRecordCount()).thenReturn(Optional.of(123));
    when(responseHandler.getContentHash()).thenAnswer(new CyclingHashAnswer());
    crawler.crawl();
    verify(client, times(3 * NUM_RANGES)).execute(anyString(), eq(responseHandler));
    verify(crawlListener, times(NUM_RANGES)).error(anyString());
  }

  /**
   * unknown end of records known record count got content in first request, then nothing on next
   * page
   *
   * <p>relying on speculative requests to skip to next range
   */
  @Test
  public void testScenario11And12And14Error() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.of(false));
    when(responseHandler.getRecordCount()).thenReturn(Optional.of(0));
    when(responseHandler.getContentHash()).thenAnswer(new CyclingHashAnswer());
    crawler.crawl();
    verify(client, times(3 * NUM_RANGES)).execute(anyString(), eq(responseHandler));
    verify(crawlListener, times(NUM_RANGES)).error(anyString());
  }

  @Test
  public void testScenario13() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.of(true));
    when(responseHandler.getRecordCount()).thenReturn(Optional.of(123));
    when(responseHandler.getContentHash()).thenReturn(Optional.of(123456L));
    crawler.crawl();
    verify(client, times(NUM_RANGES)).execute(anyString(), eq(responseHandler));
    verify(crawlListener, times(NUM_RANGES - 1)).error(anyString());
  }

  /**
   * unknown end of records unknown record count got content in first request, then nothing on next
   * page valid response
   *
   * <p>relying on speculative requests to skip to next range
   */
  @Test
  public void testScenario1And2And14() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.absent());
    when(responseHandler.getRecordCount()).thenReturn(Optional.absent());
    when(responseHandler.getContentHash()).thenAnswer(new CyclingHashAnswer());
    crawler.crawl();
    verify(client, times(3 * NUM_RANGES)).execute(anyString(), eq(responseHandler));
  }

  /**
   * unknown end of records known record count got content in first request, then nothing on next
   * page
   *
   * <p>relying on speculative requests to skip to next range
   */
  @Test
  public void testScenario3And4And14() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.absent());
    when(responseHandler.getRecordCount()).thenReturn(Optional.of(123));
    when(responseHandler.getContentHash()).thenAnswer(new CyclingHashAnswer());
    crawler.crawl();
    verify(client, times(3 * NUM_RANGES)).execute(anyString(), eq(responseHandler));
    verify(crawlListener, times(NUM_RANGES)).error(anyString());
  }

  /**
   * unknown end of records known record count got content in first request, then nothing on next
   * page
   *
   * <p>relying on speculative requests to skip to next range
   */
  @Test
  public void testScenario3And4And14Error() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.absent());
    when(responseHandler.getRecordCount()).thenReturn(Optional.of(0));
    when(responseHandler.getContentHash()).thenAnswer(new CyclingHashAnswer());
    crawler.crawl();
    verify(client, times(3 * NUM_RANGES)).execute(anyString(), eq(responseHandler));
    verify(crawlListener, times(NUM_RANGES)).error(anyString());
  }

  /** end of records unknown record count got content */
  @Test
  public void testScenario5() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.of(true));
    when(responseHandler.getRecordCount()).thenReturn(Optional.absent());
    when(responseHandler.getContentHash()).thenAnswer(new CyclingHashAnswer(Optional.of(1L)));
    crawler.crawl();
    verify(client, times(NUM_RANGES)).execute(anyString(), eq(responseHandler));
  }

  /** end of records unknown record count no content */
  @Test
  public void testScenario6() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.of(true));
    when(responseHandler.getRecordCount()).thenReturn(Optional.absent());
    when(responseHandler.getContentHash()).thenReturn(Optional.absent());
    crawler.crawl();
    verify(client, times(NUM_RANGES)).execute(anyString(), eq(responseHandler));
  }

  /** end of records known record count (!= 0) got content */
  @Test
  public void testScenario7() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.of(true));
    when(responseHandler.getRecordCount()).thenReturn(Optional.of(123));
    when(responseHandler.getContentHash()).thenAnswer(new CyclingHashAnswer(Optional.of(1L)));
    crawler.crawl();
    verify(client, times(NUM_RANGES)).execute(anyString(), eq(responseHandler));
  }

  /** end of records known record count (== 0) got content */
  @Test
  public void testScenario7Error() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.of(true));
    when(responseHandler.getRecordCount()).thenReturn(Optional.of(0));
    when(responseHandler.getContentHash()).thenAnswer(new CyclingHashAnswer(Optional.of(1L)));
    crawler.crawl();
    verify(client, times(NUM_RANGES)).execute(anyString(), eq(responseHandler));
    verify(crawlListener, times(NUM_RANGES)).error(anyString());
  }

  /** end of records known record count (== 0) no content */
  @Test
  public void testScenario8() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.of(true));
    when(responseHandler.getRecordCount()).thenReturn(Optional.of(0));
    when(responseHandler.getContentHash()).thenReturn(Optional.absent());
    crawler.crawl();
    verify(client, times(NUM_RANGES)).execute(anyString(), eq(responseHandler));
  }

  /** end of records known record count (!= 0) no content */
  @Test
  public void testScenario8Error() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.of(true));
    when(responseHandler.getRecordCount()).thenReturn(Optional.of(123));
    when(responseHandler.getContentHash()).thenReturn(Optional.absent());
    crawler.crawl();
    verify(client, times(NUM_RANGES)).execute(anyString(), eq(responseHandler));
    verify(crawlListener, times(NUM_RANGES)).error(anyString());
  }

  /**
   * unknown end of records unknown record count got content in first request, then nothing on next
   * page valid response
   *
   * <p>relying on speculative requests to skip to next range
   */
  @Test
  public void testScenario9And10And14() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.of(false));
    when(responseHandler.getRecordCount()).thenReturn(Optional.absent());
    when(responseHandler.getContentHash()).thenAnswer(new CyclingHashAnswer());
    crawler.crawl();
    verify(client, times(3 * NUM_RANGES)).execute(anyString(), eq(responseHandler));
  }

  @Test
  public void testSuccessfulSimpleCrawl() throws Exception {
    when(responseHandler.isEndOfRecords()).thenReturn(Optional.of(true));
    when(responseHandler.getRecordCount()).thenReturn(Optional.absent());
    when(responseHandler.getContentHash()).thenReturn(Optional.absent());
    crawler.crawl();
    verify(client, times(NUM_RANGES)).execute(anyString(), eq(responseHandler));
  }
}
