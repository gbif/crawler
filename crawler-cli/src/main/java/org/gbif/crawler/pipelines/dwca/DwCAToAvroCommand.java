package org.gbif.crawler.pipelines.dwca;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to convert downloaded DwCA Archive to Avro.
 * This command starts a service which listens to the {@link org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage } and perform conversion
 */
@MetaInfServices(Command.class)
public class DwCAToAvroCommand extends ServiceCommand {

  private final DwCAToAvroConfiguration config = new DwCAToAvroConfiguration();

  public DwCAToAvroCommand() {
    super("dwca-to-avro");
  }

  @Override
  protected Service getService() {
    return new DwCAToAvroService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
