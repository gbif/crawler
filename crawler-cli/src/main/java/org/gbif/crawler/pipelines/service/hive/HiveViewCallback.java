package org.gbif.crawler.pipelines.service.hive;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.crawler.pipelines.config.HiveViewConfiguration;
import org.gbif.crawler.pipelines.service.PipelineCallback;
import org.gbif.crawler.pipelines.service.PipelineCallback.Steps;

import java.util.Set;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.constants.PipelinesNodePaths.HIVE_VIEW;

import static com.google.common.base.Preconditions.checkNotNull;

/** Callback which is called when the {@link PipelinesInterpretedMessage} is received. */
public class HiveViewCallback extends AbstractMessageCallback<PipelinesInterpretedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveViewCallback.class);
  private final HiveViewConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;

  HiveViewCallback(HiveViewConfiguration config, MessagePublisher publisher, CuratorFramework curator) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.publisher = publisher;
  }

  /** Handles a MQ {@link PipelinesInterpretedMessage} message */
  @Override
  public void handleMessage(PipelinesInterpretedMessage message) {

    // Common variables
    UUID datasetId = message.getDatasetUuid();
    String attempt = Integer.toString(message.getAttempt());
    Set<String> steps = message.getPipelineSteps();

    // Main message processing logic, creates a terminal java process, which runs
    Runnable runnable = () -> {
      LOG.info("HELLO {} {} {}!", datasetId, attempt, steps.toString());
    };

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ
    // message
    PipelineCallback.create()
      .incomingMessage(message)
      .curator(curator)
      .zkRootElementPath(HIVE_VIEW)
      .pipelinesStepName(Steps.HIVE_VIEW.name())
      .publisher(publisher)
      .runnable(runnable)
      .build()
      .handleMessage();
  }
}
