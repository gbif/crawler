package org.gbif.crawler.pipelines.runner;

import java.io.IOException;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.crawler.pipelines.runner.handler.InterpretedMessageHandler;
import org.gbif.crawler.pipelines.runner.handler.VerbatimMessageHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/** Callback which is called when the {@link PipelinesBalancerMessage} is received. */
public class BalancerCallback extends AbstractMessageCallback<PipelinesBalancerMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerCallback.class);
  private final BalancerConfiguration config;
  private final MessagePublisher publisher;

  BalancerCallback(BalancerConfiguration config, MessagePublisher publisher) {
    this.config = checkNotNull(config, "config cannot be null");
    this.publisher = publisher;
  }

  /** Handles a MQ {@link PipelinesBalancerMessage} message */
  @Override
  public void handleMessage(PipelinesBalancerMessage message) {
    LOG.info("The message has been revievd - {}", message);

    String className = message.getMessageClass();
    try {
      if (PipelinesVerbatimMessage.class.getSimpleName().equals(className)) {
        VerbatimMessageHandler.handle(config, publisher, message);
      } else if (PipelinesInterpretedMessage.class.getSimpleName().equals(className)) {
        InterpretedMessageHandler.handle(config, publisher, message);
      } else {
        LOG.error("Handler for {} wasn't found!", className);
      }
    } catch (IOException ex) {
      LOG.error("Exception during balancing the message", ex);
    }
  }

}
