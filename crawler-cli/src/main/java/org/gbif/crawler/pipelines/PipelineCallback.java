package org.gbif.crawler.pipelines;

import java.util.Set;

import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.gbif.crawler.pipelines.PipelineCallback.Steps.ALL;

/**
 * Common class for building and handling a pipeline step. Contains {@link Builder} to simplify the creation process
 * and main handling process. Please see the main method {@link PipelineCallback#handleMessage}
 */
public class PipelineCallback {

  // General steps, each step is a microservice
  public enum Steps {
    ALL,
    DWCA_TO_VERBATIM,
    XML_TO_VERBATIM,
    VERBATIM_TO_INTERPRETED,
    INTERPRETED_TO_INDEX,
    HIVE_VIEW
  }

  // General runners, STANDALONE - run an app using local resources, DISTRIBUTED - run an app using YARN cluster
  public enum Runner {
    STANDALONE,
    DISTRIBUTED
  }

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
    private String pipelinesStepName;
    private String zkRootElementPath;
    private Runnable runnable;

    /**
     * @param publisher MQ message publisher
     */
    public Builder publisher(MessagePublisher publisher) {
      this.publisher = publisher;
      return this;
    }

    /**
     * @param curator Zookeeper client
     */
    public Builder curator(CuratorFramework curator) {
      this.curator = curator;
      return this;
    }

    /**
     * @param incomingMessage incoming MQ message to handle
     */
    public Builder incomingMessage(PipelineBasedMessage incomingMessage) {
      this.incomingMessage = incomingMessage;
      return this;
    }

    /**
     * @param outgoingMessage outgoing MQ message for the next pipeline step
     */
    public Builder outgoingMessage(Message outgoingMessage) {
      this.outgoingMessage = outgoingMessage;
      return this;
    }

    /**
     * @param pipelinesStepName the next pipeline step name - {@link Steps}
     */
    public Builder pipelinesStepName(String pipelinesStepName) {
      this.pipelinesStepName = pipelinesStepName;
      return this;
    }

    /**
     * @param zkRootElementPath path to store metrics information in zookeeper
     */
    public Builder zkRootElementPath(String zkRootElementPath) {
      this.zkRootElementPath = zkRootElementPath;
      return this;
    }

    /**
     * @param runnable the main process to run
     */
    public Builder runnable(Runnable runnable) {
      this.runnable = runnable;
      return this;
    }

    public PipelineCallback build() {
      return new PipelineCallback(this);
    }
  }

  /**
   * The main process handling:
   * <p>
   * 1) Receives a MQ message
   * 2) Updates Zookeeper start date monitoring metrics
   * 3) Runs runnable function, which is the main message processing logic
   * 4) Updates Zookeeper end date monitoring metrics
   * 5) Sends a wrapped message to Balancer microservice
   * 6) Updates Zookeeper successful or error monitoring metrics
   * 7) Cleans Zookeeper monitoring metrics if the received message is the last
   */
  public void handleMessage() {

    // Short variables
    PipelineBasedMessage inMessage = b.incomingMessage;
    Set<String> steps = inMessage.getPipelineSteps();

    // Check the step
    if (!steps.contains(b.pipelinesStepName) && !steps.contains(ALL.name())) {
      return;
    }

    // Start main process
    String crawlId = inMessage.getDatasetUuid().toString() + "_" + inMessage.getAttempt();
    MDC.put("crawlId", crawlId);

    LOG.info("Message has been received {}", inMessage);
    try {

      String startDatePath = Fn.START_DATE.apply(b.zkRootElementPath);
      ZookeeperUtils.updateMonitoringDate(b.curator, crawlId, startDatePath);

      LOG.info("Handler has been started, crawlId - {}", crawlId);
      b.runnable.run();
      LOG.info("Handler has been finished, crawlId - {}", crawlId);

      String endDatePath = Fn.END_DATE.apply(b.zkRootElementPath);
      ZookeeperUtils.updateMonitoringDate(b.curator, crawlId, endDatePath);

      // Send a wrapped outgoing message to Balancer queue
      if (b.outgoingMessage != null) {
        String successfulPath = Fn.SUCCESSFUL_AVAILABILITY.apply(b.zkRootElementPath);
        ZookeeperUtils.updateMonitoring(b.curator, crawlId, successfulPath, Boolean.TRUE.toString());

        String nextMessageClassName = b.outgoingMessage.getClass().getSimpleName();
        String messagePayload = b.outgoingMessage.toString();
        b.publisher.send(new PipelinesBalancerMessage(nextMessageClassName, messagePayload));

        String info = "Next message has been sent - " + b.outgoingMessage;
        LOG.info(info);

        String successfulMessagePath = Fn.SUCCESSFUL_MESSAGE.apply(b.zkRootElementPath);
        ZookeeperUtils.updateMonitoring(b.curator, crawlId, successfulMessagePath, info);
      }

      // Change zookeeper counter for passed steps
      int size = steps.contains(ALL.name()) ? Steps.values().length - 2 : steps.size();
      ZookeeperUtils.checkMonitoringById(b.curator, size, crawlId);

    } catch (Exception ex) {
      String error = "Error for crawlId - " + crawlId + " : " + ex.getMessage();
      LOG.error(error);

      String errorPath = Fn.ERROR_AVAILABILITY.apply(b.zkRootElementPath);
      ZookeeperUtils.updateMonitoring(b.curator, crawlId, errorPath, Boolean.TRUE.toString());

      String errorMessagePath = Fn.ERROR_MESSAGE.apply(b.zkRootElementPath);
      ZookeeperUtils.updateMonitoring(b.curator, crawlId, errorMessagePath, error);
    }
  }
}
