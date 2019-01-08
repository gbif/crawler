package org.gbif.crawler.pipelines.service;

import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.PipelineCallback.Steps;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.gbif.crawler.constants.PipelinesNodePaths.DWCA_TO_VERBATIM;
import static org.gbif.crawler.constants.PipelinesNodePaths.SIZE;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.gbif.crawler.pipelines.PipelineCallback.Steps.ALL;

@RunWith(MockitoJUnitRunner.class)
public class PipelineCallbackTest {

  private static CuratorFramework curator;
  private static TestingServer server;
  @Mock
  private MessagePublisher mockPublisher;

  @BeforeClass
  public static void setUp() throws Exception {
    server = new TestingServer();
    curator = CuratorFrameworkFactory.builder()
      .connectString(server.getConnectString())
      .namespace("crawler")
      .retryPolicy(new RetryOneTime(1))
      .build();
    curator.start();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    curator.close();
    server.stop();
  }

  @Test(expected = NullPointerException.class)
  public void testEmptyBuilder() {
    // When
    PipelineCallback.create().build().handleMessage();
  }

  @Test
  public void testNullMessagePublisher() throws Exception {

    // State
    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    String crawlId = datasetKey + "_" + attempt;
    String rootPath = DWCA_TO_VERBATIM;
    String nextStepName = Steps.DWCA_TO_VERBATIM.name();
    Set<String> pipelineSteps = Collections.singleton(ALL.name());
    PipelineBasedMessage incomingMessage = createMessage(datasetKey, attempt, pipelineSteps);
    Runnable runnable = () -> System.out.println("RUN!");
    Message outgoingMessage = () -> null;
    MessagePublisher publisher = null;

    // When
    PipelineCallback.create()
      .incomingMessage(incomingMessage)
      .outgoingMessage(outgoingMessage)
      .curator(curator)
      .zkRootElementPath(rootPath)
      .pipelinesStepName(nextStepName)
      .runnable(runnable)
      .publisher(publisher)
      .build()
      .handleMessage();

    // Should
    Assert.assertTrue(getAsDate(crawlId, Fn.START_DATE.apply(DWCA_TO_VERBATIM)).isPresent());
    Assert.assertTrue(getAsDate(crawlId, Fn.END_DATE.apply(DWCA_TO_VERBATIM)).isPresent());

    Assert.assertTrue(getAsBoolean(crawlId, Fn.ERROR_AVAILABILITY.apply(DWCA_TO_VERBATIM)).isPresent());
    Assert.assertTrue(getAsString(crawlId, Fn.ERROR_MESSAGE.apply(DWCA_TO_VERBATIM)).isPresent());

    Assert.assertTrue(getAsBoolean(crawlId, Fn.SUCCESSFUL_AVAILABILITY.apply(DWCA_TO_VERBATIM)).isPresent());
    Assert.assertFalse(getAsString(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(DWCA_TO_VERBATIM)).isPresent());

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  @Test
  public void testBaseHandlerBehavior() throws Exception {

    // State
    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    String crawlId = datasetKey + "_" + attempt;
    String rootPath = DWCA_TO_VERBATIM;
    String nextStepName = Steps.DWCA_TO_VERBATIM.name();
    Set<String> pipelineSteps = Collections.singleton(ALL.name());
    PipelineBasedMessage incomingMessage = createMessage(datasetKey, attempt, pipelineSteps);
    Runnable runnable = () -> System.out.println("RUN!");
    Message outgoingMessage = () -> null;

    // When
    PipelineCallback.create()
      .incomingMessage(incomingMessage)
      .outgoingMessage(outgoingMessage)
      .curator(curator)
      .zkRootElementPath(rootPath)
      .pipelinesStepName(nextStepName)
      .runnable(runnable)
      .publisher(mockPublisher)
      .build()
      .handleMessage();

    // Should
    Assert.assertTrue(getAsDate(crawlId, Fn.START_DATE.apply(DWCA_TO_VERBATIM)).isPresent());
    Assert.assertTrue(getAsDate(crawlId, Fn.END_DATE.apply(DWCA_TO_VERBATIM)).isPresent());

    Assert.assertFalse(getAsBoolean(crawlId, Fn.ERROR_AVAILABILITY.apply(DWCA_TO_VERBATIM)).isPresent());
    Assert.assertFalse(getAsString(crawlId, Fn.ERROR_MESSAGE.apply(DWCA_TO_VERBATIM)).isPresent());

    Assert.assertTrue(getAsBoolean(crawlId, Fn.SUCCESSFUL_AVAILABILITY.apply(DWCA_TO_VERBATIM)).isPresent());
    Assert.assertTrue(getAsString(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(DWCA_TO_VERBATIM)).isPresent());

    Assert.assertTrue(getAsString(crawlId, SIZE).isPresent());
    Assert.assertEquals("1", getAsString(crawlId, SIZE).get());

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  @Test
  public void testOneStepHandler() throws Exception {

    // State
    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    String crawlId = datasetKey + "_" + attempt;
    String rootPath = DWCA_TO_VERBATIM;
    String nextStepName = Steps.DWCA_TO_VERBATIM.name();
    Set<String> pipelineSteps = Collections.singleton("DWCA_TO_VERBATIM");
    PipelineBasedMessage incomingMessage = createMessage(datasetKey, attempt, pipelineSteps);
    Runnable runnable = () -> System.out.println("RUN!");
    Message outgoingMessage = () -> null;

    // When
    PipelineCallback.create()
      .incomingMessage(incomingMessage)
      .outgoingMessage(outgoingMessage)
      .curator(curator)
      .zkRootElementPath(rootPath)
      .pipelinesStepName(nextStepName)
      .runnable(runnable)
      .publisher(mockPublisher)
      .build()
      .handleMessage();

    // Should
    Assert.assertFalse(checkExists(getPipelinesInfoPath(crawlId)));

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  @Test
  public void testRunnerException() throws Exception {

    // State
    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    String crawlId = datasetKey + "_" + attempt;
    String rootPath = DWCA_TO_VERBATIM;
    String nextStepName = Steps.DWCA_TO_VERBATIM.name();
    Set<String> pipelineSteps = Collections.singleton(ALL.name());
    PipelineBasedMessage incomingMessage = createMessage(datasetKey, attempt, pipelineSteps);
    Runnable runnable = () -> {throw new RuntimeException("Oops!");};
    Message outgoingMessage = () -> null;

    // When
    PipelineCallback.create()
      .incomingMessage(incomingMessage)
      .outgoingMessage(outgoingMessage)
      .curator(curator)
      .zkRootElementPath(rootPath)
      .pipelinesStepName(nextStepName)
      .runnable(runnable)
      .publisher(mockPublisher)
      .build()
      .handleMessage();

    // Should
    Assert.assertTrue(getAsDate(crawlId, Fn.START_DATE.apply(DWCA_TO_VERBATIM)).isPresent());
    Assert.assertFalse(getAsDate(crawlId, Fn.END_DATE.apply(DWCA_TO_VERBATIM)).isPresent());

    Assert.assertTrue(getAsBoolean(crawlId, Fn.ERROR_AVAILABILITY.apply(DWCA_TO_VERBATIM)).isPresent());
    Assert.assertTrue(getAsString(crawlId, Fn.ERROR_MESSAGE.apply(DWCA_TO_VERBATIM)).isPresent());

    Assert.assertFalse(getAsBoolean(crawlId, Fn.SUCCESSFUL_AVAILABILITY.apply(DWCA_TO_VERBATIM)).isPresent());
    Assert.assertFalse(getAsString(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(DWCA_TO_VERBATIM)).isPresent());

    Assert.assertFalse(getAsString(crawlId, SIZE).isPresent());

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  @Test
  public void testLastPipelineStep() throws Exception {

    // State
    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    String crawlId = datasetKey + "_" + attempt;
    String rootPath = DWCA_TO_VERBATIM;
    String nextStepName = Steps.DWCA_TO_VERBATIM.name();
    Set<String> pipelineSteps = Collections.singleton(ALL.name());
    PipelineBasedMessage incomingMessage = createMessage(datasetKey, attempt, pipelineSteps);
    Runnable runnable = () -> System.out.println("RUN!");
    Message outgoingMessage = () -> null;

    updateMonitoring(crawlId, SIZE, String.valueOf(4));

    // When
    PipelineCallback.create()
      .incomingMessage(incomingMessage)
      .outgoingMessage(outgoingMessage)
      .curator(curator)
      .zkRootElementPath(rootPath)
      .pipelinesStepName(nextStepName)
      .runnable(runnable)
      .publisher(mockPublisher)
      .build()
      .handleMessage();

    // Should
    Assert.assertFalse(checkExists(getPipelinesInfoPath(crawlId)));

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  @Test
  public void testMonitoringStepCounter() throws Exception {

    // State
    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    String crawlId = datasetKey + "_" + attempt;
    String rootPath = DWCA_TO_VERBATIM;
    String nextStepName = Steps.DWCA_TO_VERBATIM.name();
    Set<String> pipelineSteps = Collections.singleton(ALL.name());
    PipelineBasedMessage incomingMessage = createMessage(datasetKey, attempt, pipelineSteps);
    Runnable runnable = () -> System.out.println("RUN!");
    Message outgoingMessage = () -> null;

    updateMonitoring(crawlId, SIZE, String.valueOf(2));

    // When
    PipelineCallback.create()
      .incomingMessage(incomingMessage)
      .outgoingMessage(outgoingMessage)
      .curator(curator)
      .zkRootElementPath(rootPath)
      .pipelinesStepName(nextStepName)
      .runnable(runnable)
      .publisher(mockPublisher)
      .build()
      .handleMessage();

    // Should
    Assert.assertTrue(checkExists(getPipelinesInfoPath(crawlId)));
    Assert.assertTrue(getAsString(crawlId, SIZE).isPresent());
    Assert.assertEquals("3", getAsString(crawlId, SIZE).get());

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  private PipelineBasedMessage createMessage(UUID uuid, int attempt, Set<String> pipelineSteps) {
    return new PipelineBasedMessage() {
      @Override
      public int getAttempt() {
        return attempt;
      }

      @Override
      public Set<String> getPipelineSteps() {
        return pipelineSteps;
      }

      @Override
      public UUID getDatasetUuid() {
        return uuid;
      }

      @Override
      public String getRoutingKey() {
        return "";
      }
    };
  }

  /**
   * Check exists a Zookeeper monitoring root node by crawlId
   *
   * @param crawlId root node path
   */
  private boolean checkExists(String crawlId) throws Exception {
    return curator.checkExists().forPath(crawlId) != null;
  }

  /**
   * Removes a Zookeeper monitoring root node by crawlId
   *
   * @param crawlId root node path
   */
  private void deleteMonitoringById(String crawlId) throws Exception {
    String path = getPipelinesInfoPath(crawlId);
    if (checkExists(path)) {
      curator.delete().deletingChildrenIfNeeded().forPath(path);
    }
  }

  /**
   * Creates or updates a String value for a Zookeeper monitoring node
   *
   * @param crawlId root node path
   * @param path    child node path
   * @param value   some String value
   */
  private void updateMonitoring(String crawlId, String path, String value) throws Exception {
    String fullPath = getPipelinesInfoPath(crawlId, path);
    byte[] bytes = value.getBytes(Charsets.UTF_8);
    if (checkExists(fullPath)) {
      curator.setData().forPath(fullPath, bytes);
    } else {
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(fullPath, bytes);
    }
  }

  /**
   * Read value from Zookeeper as a {@link String}
   */
  private Optional<String> getAsString(String crawlId, String path) throws Exception {
    String infoPath = getPipelinesInfoPath(crawlId, path);
    if (curator.checkExists().forPath(infoPath) != null) {
      byte[] responseData = curator.getData().forPath(infoPath);
      if (responseData != null) {
        return Optional.of(new String(responseData, Charsets.UTF_8));
      }
    }
    return Optional.empty();
  }

  /**
   * Read value from Zookeeper as a {@link LocalDateTime}
   */
  private Optional<LocalDateTime> getAsDate(String crawlId, String path) throws Exception {
    Optional<String> data = getAsString(crawlId, path);
    try {
      return data.map(x -> LocalDateTime.parse(x, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    } catch (DateTimeParseException ex) {
      return Optional.empty();
    }
  }

  /**
   * Read value from Zookeeper as a {@link Boolean}
   */
  private Optional<Boolean> getAsBoolean(String crawlId, String path) throws Exception {
    Optional<String> data = getAsString(crawlId, path);
    try {
      return data.map(Boolean::parseBoolean);
    } catch (DateTimeParseException ex) {
      return Optional.empty();
    }
  }

}
