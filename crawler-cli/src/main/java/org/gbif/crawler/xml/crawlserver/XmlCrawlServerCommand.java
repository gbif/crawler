package org.gbif.crawler.xml.crawlserver;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
public class XmlCrawlServerCommand extends ServiceCommand {

  private final XmlCrawlServerConfiguration config = new XmlCrawlServerConfiguration();

  public XmlCrawlServerCommand() {
    super("crawlserver");
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  @Override
  protected Object getParameterObject() {
    return config;
  }

  @Override
  protected Service getService() {
    return new XmlCrawlServerService(config);
  }

}
