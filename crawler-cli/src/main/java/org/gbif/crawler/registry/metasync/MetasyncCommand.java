package org.gbif.crawler.registry.metasync;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
public class MetasyncCommand extends ServiceCommand {

  private final MetasyncConfiguration configuration = new MetasyncConfiguration();

  public MetasyncCommand() {
    super("metasync");
  }

  @Override
  protected Service getService() {
    return new MetasyncService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

}
