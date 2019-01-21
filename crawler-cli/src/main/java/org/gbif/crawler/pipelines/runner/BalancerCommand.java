package org.gbif.crawler.pipelines.runner;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

/**
 * Entry class for cli command, to start service to index interpreted dataset
 * This command starts a service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesBalancerMessage
 * } and perform conversion
 */
@MetaInfServices(Command.class)
public class BalancerCommand extends ServiceCommand {

  private final BalancerConfiguration config = new BalancerConfiguration();

  public BalancerCommand() {
    super("pipelines-balancer");
  }

  @Override
  protected Service getService() {
    return new BalancerService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
