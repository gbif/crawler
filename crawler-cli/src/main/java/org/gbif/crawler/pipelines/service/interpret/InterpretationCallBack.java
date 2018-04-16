package org.gbif.crawler.pipelines.interpret;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.ExtendedRecordAvailableMessage;
import org.gbif.crawler.pipelines.service.dwca.DwCAToAvroCallBack;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Call back which is called when the {@link org.gbif.common.messaging.api.messages.ExtendedRecordAvailableMessage } is received.
 */
public class InterpretationCallBack extends AbstractMessageCallback<ExtendedRecordAvailableMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(DwCAToAvroCallBack.class);
  private final InterpreterConfiguration configuration;

  InterpretationCallBack(InterpreterConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public void handleMessage(ExtendedRecordAvailableMessage extendedRecordAvailableMessage) {
    Log.info("Message received: " + extendedRecordAvailableMessage);
  }
}
