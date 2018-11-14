package org.gbif.crawler.pipelines.service.interpret;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.crawler.pipelines.config.InterpreterConfiguration;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to interpret dataset available as avro.
 * This command starts a service which listens to the {@link org.gbif.common.messaging.api.messages.ExtendedRecordAvailableMessage } and perform interpretation
 */
@MetaInfServices(Command.class)
public class InterpretationCommand extends ServiceCommand {

  private final InterpreterConfiguration config = new InterpreterConfiguration();

  public InterpretationCommand() {
    super("interpret-dataset");
  }

  @Override
  protected Service getService() { return new InterpretationService(config); }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

}