package org.gbif.crawler.dwca.metasync;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
public class DwcaMetasyncCommand extends ServiceCommand {

  private final DwcaMetasyncConfiguration configuration = new DwcaMetasyncConfiguration();

  public DwcaMetasyncCommand() {
    super("dwca-metasync");
  }

  @Override
  protected Service getService() {
    return new DwcaMetasyncService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

}
