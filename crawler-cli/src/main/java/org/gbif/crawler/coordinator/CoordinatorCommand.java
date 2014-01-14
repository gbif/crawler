package org.gbif.crawler.coordinator;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
public class CoordinatorCommand extends ServiceCommand {

  private final CoordinatorConfiguration config = new CoordinatorConfiguration();

  public CoordinatorCommand() {
    super("coordinator");
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  @Override
  protected Service getService() {
    return new CoordinatorService(config);
  }

}
