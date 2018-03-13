package org.gbif.crawler.dwca.avroconverter;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DwcaDownloadFinishedMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Call back which is called when the {@link org.gbif.common.messaging.api.messages.DwcaDownloadFinishedMessage } is received.
 * ToDo Implement DwCAToAvro Conversion
 */
public class DwCAToAvroConverterCallBack extends AbstractMessageCallback<DwcaDownloadFinishedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(DwCAToAvroConverterCallBack.class);

  @Override
  public void handleMessage(DwcaDownloadFinishedMessage dwcaDownloadFinishedMessage) {
    LOG.info("Received Download finished message, " + dwcaDownloadFinishedMessage.toString());
  }
}
