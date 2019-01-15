package org.gbif.crawler.pipelines.xml;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

/**
 * CLI {@link Command} to convert XML files to Avro.
 */
@MetaInfServices(Command.class)
public class XmlToAvroCommand extends ServiceCommand {

  private final XmlToAvroConfiguration configuration = new XmlToAvroConfiguration();

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