package org.gbif.crawler.xml.propagater;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
public class AvroPropagatorCommand extends ServiceCommand {

  private final AvroPropagatorConfiguration configuration = new AvroPropagatorConfiguration();

  public AvroPropagatorCommand() {
    super("propagator");
  }

  @Override
  protected Service getService() {
    return new AvroPropagatorService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }
}
