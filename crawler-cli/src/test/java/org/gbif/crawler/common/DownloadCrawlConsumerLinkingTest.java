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
package org.gbif.crawler.common;

import org.gbif.api.model.crawler.CrawlJob;
import org.gbif.api.model.crawler.ProcessState;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.DatasetBasedMessage;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_REASON;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_CHECKLIST;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_OCCURRENCE;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_SAMPLE;
import static org.gbif.crawler.constants.CrawlerNodePaths.getCrawlInfoPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the duplicate/concurrent-attempt handling in {@link DownloadCrawlConsumer}: a second run
 * processing the same crawl attempt must not re-trigger downstream processing (no {@code
 * FileAlreadyExistsException} stack trace, no double "download finished" message), and must not
 * guess at a {@code FINISHED_REASON} it can't actually know.
 *
 * <p>This intentionally does not exercise {@link DownloadCrawlConsumer#crawl} end to end (that
 * needs a real HTTP download, see the disabled, manual {@link DownloadCrawlConsumerTest}) - it
 * tests the two extracted methods directly against a real embedded ZooKeeper.
 */
public class DownloadCrawlConsumerLinkingTest {

  private static TestingServer zkServer;
  private static CuratorFramework curator;

  @TempDir File archiveRepository;

  private TestDownloadCrawlConsumer consumer;

  @BeforeAll
  public static void setupZk() throws Exception {
    zkServer = new TestingServer();
    curator =
      CuratorFrameworkFactory.builder()
        .connectString(zkServer.getConnectString())
        .namespace("crawler-linking-test")
        .retryPolicy(new RetryOneTime(100))
        .build();
    curator.start();
  }

  @AfterAll
  public static void tearDownZk() throws IOException {
    curator.close();
    zkServer.stop();
  }

  @BeforeEach
  public void setup() {
    consumer = new TestDownloadCrawlConsumer(curator, archiveRepository);
  }

  @Test
  public void testLinkAttemptFileCreatesNewLink() throws Exception {
    UUID datasetKey = UUID.randomUUID();
    CrawlJob crawlJob = testCrawlJob(datasetKey);
    File localFile = downloadedFile(datasetKey);

    boolean linked = consumer.linkAttemptFile(archiveRepository, datasetKey, crawlJob, localFile);

    assertTrue(linked);
    assertTrue(new File(archiveRepository, datasetKey + ".1.suffix").exists());
  }

  @Test
  public void testLinkAttemptFileDetectsDuplicate() throws Exception {
    UUID datasetKey = UUID.randomUUID();
    CrawlJob crawlJob = testCrawlJob(datasetKey);
    File localFile = downloadedFile(datasetKey);

    assertTrue(consumer.linkAttemptFile(archiveRepository, datasetKey, crawlJob, localFile));
    // Simulates a second, concurrent run processing the exact same attempt.
    boolean linkedAgain = consumer.linkAttemptFile(archiveRepository, datasetKey, crawlJob, localFile);

    assertFalse(linkedAgain);
  }

  @Test
  public void testDuplicateAttemptMarksFinishedWithoutWritingFinishReason() throws Exception {
    UUID datasetKey = UUID.randomUUID();
    CrawlJob crawlJob = testCrawlJob(datasetKey);

    consumer.duplicateAttempt(datasetKey, crawlJob);

    assertEquals(ProcessState.FINISHED, readProcessState(datasetKey, PROCESS_STATE_OCCURRENCE));
    assertEquals(ProcessState.FINISHED, readProcessState(datasetKey, PROCESS_STATE_CHECKLIST));
    assertEquals(ProcessState.FINISHED, readProcessState(datasetKey, PROCESS_STATE_SAMPLE));
    // Deliberately not written - see duplicateAttempt()'s javadoc: we can't tell whether we're
    // racing ahead of or behind the genuine run, so we mustn't guess a reason for it.
    assertNull(curator.checkExists().forPath(getCrawlInfoPath(datasetKey, FINISHED_REASON)));
  }

  private static CrawlJob testCrawlJob(UUID datasetKey) {
    return new CrawlJob(
      datasetKey, EndpointType.DWC_ARCHIVE, URI.create("http://example.org/archive.zip"), 1, null);
  }

  private File downloadedFile(UUID datasetKey) throws IOException {
    File localFile = new File(archiveRepository, datasetKey + ".suffix");
    Files.write(localFile.toPath(), "content".getBytes(StandardCharsets.UTF_8));
    return localFile;
  }

  private static ProcessState readProcessState(UUID datasetKey, String subPath) throws Exception {
    byte[] data = curator.getData().forPath(getCrawlInfoPath(datasetKey, subPath));
    return ProcessState.valueOf(new String(data, StandardCharsets.UTF_8));
  }

  /** Minimal concrete subclass with no network dependency, exposing the methods under test. */
  private static class TestDownloadCrawlConsumer extends DownloadCrawlConsumer {
    TestDownloadCrawlConsumer(CuratorFramework curator, File archiveRepository) {
      super(curator, null, archiveRepository, 1000);
    }

    @Override
    protected DatasetBasedMessage createFinishedMessage(CrawlJob crawlJob) {
      return null;
    }

    @Override
    protected String getSuffix() {
      return ".suffix";
    }

    @Override
    protected File getArchiveDirectory(File archiveRepository, UUID datasetKey) {
      return archiveRepository;
    }
  }
}
