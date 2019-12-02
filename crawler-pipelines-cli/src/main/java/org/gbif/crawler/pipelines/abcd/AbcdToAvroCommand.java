package org.gbif.crawler.pipelines.abcd;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.crawler.pipelines.xml.XmlToAvroConfiguration;

import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

/**
 * CLI {@link Command} to convert XML files (ABCD archives) to Avro.
 */
@MetaInfServices(Command.class)
public class AbcdToAvroCommand extends ServiceCommand {

  private final XmlToAvroConfiguration configuration = new XmlToAvroConfiguration();

  public AbcdToAvroCommand() {
    super("pipelines-to-avro-from-abcd");
  }

  @Override
  protected Service getService() {
    return new AbcdToAvroService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }
}
