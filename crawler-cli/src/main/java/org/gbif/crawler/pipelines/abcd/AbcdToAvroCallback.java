package org.gbif.crawler.pipelines.abcd;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.PipelineCallback.Steps;
import org.gbif.crawler.pipelines.xml.XmlToAvroCallback;
import org.gbif.crawler.pipelines.xml.XmlToAvroConfiguration;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.collect.Sets;

import static org.gbif.crawler.constants.PipelinesNodePaths.ABCD_TO_VERBATIM;

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

    // Workaround to wait fs
    try {
      TimeUnit.SECONDS.sleep(30);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex.getCause());
    }

    UUID datasetId = message.getDatasetUuid();
    Integer attempt = message.getAttempt();

    MDC.put("datasetId", datasetId.toString());
    MDC.put("attempt", attempt.toString());
    LOG.info("Message handler began - {}", message);

    if (message.getPipelineSteps().isEmpty()) {
      message.setPipelineSteps(Sets.newHashSet(
          Steps.ABCD_TO_VERBATIM.name(),
          Steps.VERBATIM_TO_INTERPRETED.name(),
          Steps.INTERPRETED_TO_INDEX.name()
      ));
    }

    // Common variables
    Set<String> steps = message.getPipelineSteps();
    Runnable runnable = XmlToAvroCallback.createRunnable(config, datasetId, attempt.toString());
    EndpointType endpointType = message.getEndpointType();

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
    PipelineCallback.create()
        .incomingMessage(message)
        .outgoingMessage(new PipelinesVerbatimMessage(datasetId, attempt, config.interpretTypes, steps, endpointType))
        .curator(curator)
        .zkRootElementPath(ABCD_TO_VERBATIM)
        .pipelinesStepName(Steps.ABCD_TO_VERBATIM.name())
        .publisher(publisher)
        .runnable(runnable)
        .build()
        .handleMessage();

    LOG.info("Message handler ended - {}", message);
  }
}
