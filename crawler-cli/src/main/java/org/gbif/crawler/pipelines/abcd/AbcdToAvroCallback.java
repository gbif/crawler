package org.gbif.crawler.pipelines.abcd;

import org.gbif.api.model.crawler.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.xml.XmlToAvroCallback;
import org.gbif.crawler.pipelines.xml.XmlToAvroConfiguration;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Call back which is called when the {@link PipelinesXmlMessage} is received.
 * <p>
 * The main method is {@link AbcdToAvroCallback#handleMessage}
 */
public class AbcdToAvroCallback extends AbstractMessageCallback<PipelinesAbcdMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(AbcdToAvroCallback.class);

  private final XmlToAvroConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;

  public AbcdToAvroCallback(XmlToAvroConfiguration config, MessagePublisher publisher, CuratorFramework curator) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.publisher = publisher;
  }

  /**
   * Handles a MQ {@link PipelinesAbcdMessage} message
   */
  @Override
  public void handleMessage(PipelinesAbcdMessage message) {

    UUID datasetId = message.getDatasetUuid();
    Integer attempt = message.getAttempt();

    try (MDCCloseable mdc1 = MDC.putCloseable("datasetId", datasetId.toString());
        MDCCloseable mdc2 = MDC.putCloseable("attempt", attempt.toString())) {

      LOG.info("Message handler began - {}", message);

      if (!message.isModified()) {
        LOG.info("Skip the message, cause it wasn't modified, exit from handler");
        return;
      }

      // Workaround to wait fs
      try {
        LOG.info("Waiting 20 seconds...");
        TimeUnit.SECONDS.sleep(20);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex.getCause());
      }

      if (message.getPipelineSteps().isEmpty()) {
        message.setPipelineSteps(Sets.newHashSet(
            StepType.ABCD_TO_VERBATIM.name(),
            StepType.VERBATIM_TO_INTERPRETED.name(),
            StepType.INTERPRETED_TO_INDEX.name(),
            StepType.HIVE_VIEW.name()
        ));
      }

      // Common variables
      Set<String> steps = message.getPipelineSteps();
      EndpointType endpointType = message.getEndpointType();
      Runnable runnable = XmlToAvroCallback.createRunnable(config, datasetId, attempt.toString(), endpointType);

      // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
      PipelineCallback.create()
          .incomingMessage(message)
          .outgoingMessage(new PipelinesVerbatimMessage(datasetId, attempt, config.interpretTypes, steps, endpointType))
          .curator(curator)
          .zkRootElementPath(StepType.ABCD_TO_VERBATIM.getLabel())
          .pipelinesStepName(StepType.ABCD_TO_VERBATIM)
          .publisher(publisher)
          .runnable(runnable)
          .build()
          .handleMessage();

      LOG.info("Message handler ended - {}", message);
    }
  }
}
