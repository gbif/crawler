package org.gbif.crawler.coldp.downloader;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.crawler.coldp.ColDpConfiguration;

import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

@MetaInfServices(Command.class)
public class DownloaderCommand extends ServiceCommand {

  private final ColDpConfiguration configuration = new ColDpConfiguration();

  public DownloaderCommand() {
    super("coldpdownloader");
  }

  @Override
  protected Service getService() {
    return new DownloaderService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }
}

