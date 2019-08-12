package org.gbif.crawler.pipelines.hive;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

/**
 * Entry class for cli command, to start service to process Hive View
 * This command starts a service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage }
 */
@MetaInfServices(Command.class)
public class HiveViewCommand extends ServiceCommand {

  private final HiveViewConfiguration config = new HiveViewConfiguration();

  public HiveViewCommand() {
    super("pipelines-hive-view");
  }

  @Override
  protected Service getService() {
    return new HiveViewService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
