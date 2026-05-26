package org.gbif.crawler.dwcdp.metasync;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

@MetaInfServices(Command.class)
public class DwcDpMetasyncCommand extends ServiceCommand {

  private final DwcDpMetasyncConfiguration configuration = new DwcDpMetasyncConfiguration();

  public DwcDpMetasyncCommand() {
    super("dwcdp-metasync");
  }

  @Override
  protected Service getService() {
    return new DwcDpMetasyncService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }
}
