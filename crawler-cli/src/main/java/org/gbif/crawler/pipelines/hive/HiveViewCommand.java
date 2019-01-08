package org.gbif.crawler.pipelines.hive;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to index interpreted dataset
 * This command starts a service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage } and perform conversion
 */
@MetaInfServices(Command.class)
public class HiveViewCommand extends ServiceCommand {

  private final HiveViewConfiguration config = new HiveViewConfiguration();

  public HiveViewCommand() {
    super("hive-view");
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
