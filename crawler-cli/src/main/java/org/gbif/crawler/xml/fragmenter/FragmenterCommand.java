package org.gbif.crawler.xml.fragmenter;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
public class FragmenterCommand extends ServiceCommand {

  private final FragmenterConfiguration configuration = new FragmenterConfiguration();

  public FragmenterCommand() {
    super("fragmenter");
  }

  @Override
  protected Service getService() {
    return new FragmenterService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

}
