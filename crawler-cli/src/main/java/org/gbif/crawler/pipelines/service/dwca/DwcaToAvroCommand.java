package org.gbif.crawler.pipelines.service.dwca;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.crawler.pipelines.config.ConverterConfiguration;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to convert downloaded DwCA Archive to Avro.
 * This command starts a service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesDwcaMessage } and perform conversion
 */
@MetaInfServices(Command.class)
public class DwcaToAvroCommand extends ServiceCommand {

  private final ConverterConfiguration config = new ConverterConfiguration();

  public DwcaToAvroCommand() {
    super("dwca-to-avro");
  }

  @Override
  protected Service getService() {
    return new DwcaToAvroService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
