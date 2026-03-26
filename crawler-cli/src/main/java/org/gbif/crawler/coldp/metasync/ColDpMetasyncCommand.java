package org.gbif.crawler.coldp.metasync;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

@MetaInfServices(Command.class)
public class ColDpMetasyncCommand extends ServiceCommand {

  private final ColDpMetasyncConfiguration configuration = new ColDpMetasyncConfiguration();

  public ColDpMetasyncCommand() {
    super("coldp-metasync");
  }

  @Override
  protected Service getService() {
    return new ColDpMetasyncService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }
}
