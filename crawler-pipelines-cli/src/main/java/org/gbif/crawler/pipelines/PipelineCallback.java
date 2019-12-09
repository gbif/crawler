package org.gbif.crawler.pipelines;

import java.util.Optional;
import java.util.Set;

import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.crawler.common.utils.ZookeeperUtils;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;

/**
 * Common class for building and handling a pipeline step. Contains {@link Builder} to simplify the creation process
 * and main handling process. Please see the main method {@link PipelineCallback#handleMessage}
 */
public class PipelineCallback {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineCallback.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
    private StepType pipelinesStepName;
    private String zkRootElementPath;
    private Runnable runnable;
    private PipelinesHistoryWsClient historyWsClient;

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
     * @param pipelinesStepName the next pipeline step name - {@link StepType}
     */
    public Builder pipelinesStepName(StepType pipelinesStepName) {
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

    /**
     * @param historyWsClient ws client to track the history of pipelines processes
     */
    public Builder historyWsClient(PipelinesHistoryWsClient historyWsClient) {
      this.historyWsClient = historyWsClient;
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
   * 3) Create pipeline step in tracking service
   * 4) Runs runnable function, which is the main message processing logic
   * 5) Updates Zookeeper end date monitoring metrics
   * 6) Update status in tracking service
   * 7) Sends a wrapped message to Balancer microservice
   * 8) Updates Zookeeper successful or error monitoring metrics
   * 9) Cleans Zookeeper monitoring metrics if the received message is the last
   */
  public void handleMessage() {

    // Short variables
    PipelineBasedMessage inMessage = b.incomingMessage;
    Set<String> steps = inMessage.getPipelineSteps();

    // Check the step
    if (!steps.contains(b.pipelinesStepName.name())) {
      return;
    }

    // Start main process
    String crawlId = inMessage.getDatasetUuid().toString() + "_" + inMessage.getAttempt();
    Optional<TrackingInfo> trackingInfo = Optional.empty();

    try (MDCCloseable mdc = MDC.putCloseable("crawlId", crawlId)) {

      LOG.info("Message has been received {}", inMessage);
      if (ZookeeperUtils.checkExists(b.curator, getPipelinesInfoPath(crawlId, b.zkRootElementPath))) {
        LOG.warn("Dataset is already in pipelines queue, please check the pipeline-ingestion monitoring tool - {}", crawlId);
        return;
      }
      String mqMessagePath = Fn.MQ_MESSAGE.apply(b.zkRootElementPath);
      ZookeeperUtils.updateMonitoring(b.curator, crawlId, mqMessagePath, inMessage.toString());

      String mqClassNamePath = Fn.MQ_CLASS_NAME.apply(b.zkRootElementPath);
      ZookeeperUtils.updateMonitoring(b.curator, crawlId, mqClassNamePath, inMessage.getClass().getCanonicalName());

      String startDatePath = Fn.START_DATE.apply(b.zkRootElementPath);
      ZookeeperUtils.updateMonitoringDate(b.curator, crawlId, startDatePath);

      String runnerPath = Fn.RUNNER.apply(b.zkRootElementPath);
      ZookeeperUtils.updateMonitoring(b.curator, crawlId, runnerPath, getRunner(inMessage));

      // track the pipeline step
      trackingInfo = trackPipelineStep();

      LOG.info("Handler has been started, crawlId - {}", crawlId);
      b.runnable.run();
      LOG.info("Handler has been finished, crawlId - {}", crawlId);

      String endDatePath = Fn.END_DATE.apply(b.zkRootElementPath);
      ZookeeperUtils.updateMonitoringDate(b.curator, crawlId, endDatePath);

      // update tracking status
      trackingInfo.ifPresent(info -> updateTrackingStatus(info, PipelineStep.Status.COMPLETED));

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
      ZookeeperUtils.checkMonitoringById(b.curator, steps.size(), crawlId);

    } catch (Exception ex) {
      String error = "Error for crawlId - " + crawlId + " : " + ex.getMessage();
      LOG.error(error, ex);

      String errorPath = Fn.ERROR_AVAILABILITY.apply(b.zkRootElementPath);
      ZookeeperUtils.updateMonitoring(b.curator, crawlId, errorPath, Boolean.TRUE.toString());

      String errorMessagePath = Fn.ERROR_MESSAGE.apply(b.zkRootElementPath);
      ZookeeperUtils.updateMonitoring(b.curator, crawlId, errorMessagePath, error);

      // update tracking status
      trackingInfo.ifPresent(info -> updateTrackingStatus(info, PipelineStep.Status.FAILED));
    }
  }

  private Optional<TrackingInfo> trackPipelineStep() {
    try {
      // create pipeline process. If it already exists it returns the existing one (the db query does an upsert).
      long processKey =
          b.historyWsClient.createPipelineProcess(
              b.incomingMessage.getDatasetUuid(), b.incomingMessage.getAttempt());

      // add step to the process
      PipelineStep step =
          new PipelineStep()
              .setMessage(OBJECT_MAPPER.writeValueAsString(b.incomingMessage))
              .setType(b.pipelinesStepName)
              .setState(PipelineStep.Status.RUNNING)
              .setRunner(StepRunner.valueOf(getRunner(b.incomingMessage)));
      long stepKey = b.historyWsClient.addPipelineStep(processKey, step);

      return Optional.of(new TrackingInfo(processKey, stepKey));
    } catch (Exception ex) {
      // we don't want to break the crawling if the tracking fails
      LOG.error("Couldn't track pipeline step for message {}", b.incomingMessage, ex);
      return Optional.empty();
    }
  }

  private void updateTrackingStatus(TrackingInfo trackingInfo, PipelineStep.Status status) {
    try {
      b.historyWsClient.updatePipelineStepStatusAndMetrics(trackingInfo.processKey, trackingInfo.stepKey, status);
    } catch (Exception ex) {
      // we don't want to break the crawling if the tracking fails
      LOG.error(
          "Couldn't update tracking status for process {} and step {}",
          trackingInfo.processKey,
          trackingInfo.stepKey,
          ex);
    }
  }

  private String getRunner(PipelineBasedMessage inMessage) {

    if (inMessage instanceof PipelinesAbcdMessage
        || inMessage instanceof PipelinesXmlMessage
        || inMessage instanceof PipelinesDwcaMessage) {
      return StepRunner.STANDALONE.name();
    }

    if (inMessage instanceof PipelinesIndexedMessage) {
      return ((PipelinesIndexedMessage) inMessage).getRunner();
    }

    if (inMessage instanceof PipelinesInterpretedMessage) {
      return ((PipelinesInterpretedMessage) inMessage).getRunner();
    }

    if (inMessage instanceof PipelinesVerbatimMessage) {
      return ((PipelinesVerbatimMessage) inMessage).getRunner();
    }

    return StepRunner.UNKNOWN.name();
  }

  private static class TrackingInfo {
    long processKey;
    long stepKey;

    TrackingInfo(long processKey, long stepKey) {
      this.processKey = processKey;
      this.stepKey = stepKey;
    }
  }
}
