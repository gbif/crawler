package org.gbif.crawler.dwca.avroconverter;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Call back which is called when the {@link org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage } is received.
 * ToDo Implement DwCAToAvro Conversion
 */
public class DwCAToAvroConverterCallBack extends AbstractMessageCallback<DwcaValidationFinishedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(DwCAToAvroConverterCallBack.class);

  @Override
  public void handleMessage(DwcaValidationFinishedMessage dwcaValidationFinishedMessage) {
    LOG.info("Received Download finished message {}", dwcaValidationFinishedMessage.toString());
  }
}
