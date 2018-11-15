package org.gbif.crawler.pipelines.service;

import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Set;

import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.gbif.crawler.pipelines.service.PipelineCallback.Steps.ALL;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

/**
 * Common class for building and handling a pipeline step. Contains {@link Builder} to simplify the creation process
 * and main handling process. Please see {@link PipelineCallback#handleMessage()}
 */
public class PipelineCallback {

  public enum Steps {ALL, VERBATIM_TO_INTERPRETED, INTERPRETED_TO_INDEX, INDEXING_FINISHED}

  private static final Logger LOG = LoggerFactory.getLogger(PipelineCallback.class);

  private final Builder b;

  private PipelineCallback(Builder b) {
    this.b = b;
  }

  public static Builder create() {
    return new PipelineCallback.Builder();
  }

  public static class Builder {

    private Builder() {
      // NOP
    }

    private MessagePublisher publisher;
    private CuratorFramework curator;
    private PipelineBasedMessage incomingMessage;
    private Message outgoingMessage;
    private String nextPipelinesStep;
    private String zkRootElementPath;
    private Runnable runnable;

    public Builder publisher(MessagePublisher publisher) {
      this.publisher = publisher;
      return this;
    }

    public Builder curator(CuratorFramework curator) {
      this.curator = curator;
      return this;
    }

    public Builder incomingMessage(PipelineBasedMessage incomingMessage) {
      this.incomingMessage = incomingMessage;
      return this;
    }

    public Builder outgoingMessage(Message outgoingMessage) {
      this.outgoingMessage = outgoingMessage;
      return this;
    }

    public Builder nextPipelinesStep(String nextPipelinesStep) {
      this.nextPipelinesStep = nextPipelinesStep;
      return this;
    }

    public Builder zkRootElementPath(String zkRootElementPath) {
      this.zkRootElementPath = zkRootElementPath;
      return this;
    }

    public Builder runnable(Runnable runnable) {
      this.runnable = runnable;
      return this;
    }

    public PipelineCallback build() {
      return new PipelineCallback(this);
    }
  }

  /**
   * The main handling process:
   * <p>
   * 1) Receives a MQ message
   * 2) Updates Zookeeper start date monitoring metrics
   * 3) Runs runnable function, which is the main message processing logic
   * 4) Updates Zookeeper end date monitoring metrics
   * 5) Sends a message to the next MQ listener
   * 6) Updates Zookeeper successful or error monitoring metrics
   * 7) Cleans Zookeeper monitoring metrics if the received message is the last
   */
  public void handleMessage() {
    LOG.info("Message has been received {}", b.incomingMessage);

    String crawlId = b.incomingMessage.getDatasetUuid().toString() + "_" + b.incomingMessage.getAttempt();
    try {

      updateMonitoringDate(crawlId, Fn.START_DATE.apply(b.zkRootElementPath));

      LOG.info("Handler has been started, crawlId - {}", crawlId);
      b.runnable.run();
      LOG.info("Handler has been finished, crawlId - {}", crawlId);

      updateMonitoringDate(crawlId, Fn.END_DATE.apply(b.zkRootElementPath));

      Set<String> steps = b.incomingMessage.getPipelineSteps();

      // Send incomingMessage to MQ
      if (steps.contains(b.nextPipelinesStep) || steps.contains(ALL.name())) {

        updateMonitoring(crawlId, Fn.SUCCESSFUL_AVAILABILITY.apply(b.zkRootElementPath), Boolean.TRUE.toString());

        b.publisher.send(b.outgoingMessage);

        String info = "Next message has been sent - " + b.outgoingMessage;

        LOG.info(info);

        updateMonitoring(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(b.zkRootElementPath), info);

      } else {
        LOG.info("Delete zookeeper node, crawlId - {}", crawlId);
        deleteMonitoringById(crawlId);
      }

    } catch (Exception ex) {
      String error = "Could not send incomingMessage for crawlId - " + crawlId + " : " + ex.getMessage();
      LOG.error(error);

      updateMonitoring(crawlId, Fn.ERROR_AVAILABILITY.apply(b.zkRootElementPath), Boolean.TRUE.toString());
      updateMonitoring(crawlId, Fn.ERROR_MESSAGE.apply(b.zkRootElementPath), error);
    }

  }

  /**
   * Check exists a Zookeeper monitoring root node by crawlId
   *
   * @param crawlId root node path
   */
  private boolean checkExists(String crawlId) throws Exception {
    return b.curator.checkExists().forPath(crawlId) != null;
  }

  /**
   * Removes a Zookeeper monitoring root node by crawlId
   *
   * @param crawlId root node path
   */
  private void deleteMonitoringById(String crawlId) {
    try {
      String path = getPipelinesInfoPath(crawlId);
      if (checkExists(path)) {
        b.curator.delete().deletingChildrenIfNeeded().forPath(path);
      }
    } catch (Exception ex) {
      LOG.error("Exception while updating ZooKeeper", ex);
    }
  }

  /**
   * Creates or updates a String value for a Zookeeper monitoring node
   *
   * @param crawlId root node path
   * @param path      child node path
   * @param value     some String value
   */
  private void updateMonitoring(String crawlId, String path, String value) {
    try {
      String fullPath = getPipelinesInfoPath(crawlId, path);
      byte[] bytes = value.getBytes(Charsets.UTF_8);
      if (checkExists(fullPath)) {
        b.curator.setData().forPath(fullPath, bytes);
      } else {
        b.curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(fullPath, bytes);
      }
    } catch (Exception ex) {
      LOG.error("Exception while updating ZooKeeper", ex);
    }
  }

  /**
   * Creates or updates current LocalDateTime value for a Zookeeper monitoring node
   *
   * @param crawlId root node path
   * @param path      child node path
   */
  private void updateMonitoringDate(String crawlId, String path) {
    String value = LocalDateTime.now(ZoneOffset.UTC).format(ISO_LOCAL_DATE_TIME);
    updateMonitoring(crawlId, path, value);
  }
}
