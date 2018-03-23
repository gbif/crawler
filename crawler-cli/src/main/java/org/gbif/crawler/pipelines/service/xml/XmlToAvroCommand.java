package org.gbif.crawler.pipelines.service.xml;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.crawler.pipelines.ConverterConfiguration;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

/**
 * CLI {@link Command} to convert XML files to Avro.
 */
@MetaInfServices(Command.class)
public class XmlToAvroCommand extends ServiceCommand {

  private final ConverterConfiguration configuration = new ConverterConfiguration();

  public XmlToAvroCommand() {
    super("xml-to-avro");
  }

  @Override
  protected Service getService() {
    return new XmlToAvroService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }
}
