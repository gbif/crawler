package org.gbif.crawler.pipelines.runner.handler;

import java.io.IOException;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.crawler.pipelines.runner.BalancerConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class PipelinesIndexedMessageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(VerbatimMessageHandler.class);

  private PipelinesIndexedMessageHandler() {
  }

  public static void handle(BalancerConfiguration config, MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    LOG.info("Process PipelinesIndexedMessage - {}", message);

    ObjectMapper mapper = new ObjectMapper();
    PipelinesIndexedMessage m = mapper.readValue(message.getPayload(), PipelinesIndexedMessage.class);
    publisher.send(
        new PipelinesIndexedMessage(m.getDatasetUuid(), m.getAttempt(), m.getPipelineSteps(), m.getRunner()));
  }
}
