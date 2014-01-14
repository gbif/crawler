package org.gbif.crawler.dwca.validator;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
public class ValidatorCommand extends ServiceCommand {

  private final DwcaValidatorConfiguration configuration = new DwcaValidatorConfiguration();

  public ValidatorCommand() {
    super("validator");
  }

  @Override
  protected Service getService() {
    return new ValidatorService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

}
