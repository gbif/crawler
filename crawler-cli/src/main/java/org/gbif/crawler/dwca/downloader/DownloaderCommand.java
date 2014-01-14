package org.gbif.crawler.dwca.downloader;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
public class DownloaderCommand extends ServiceCommand {

  private final DownloaderConfiguration configuration = new DownloaderConfiguration();

  public DownloaderCommand() {
    super("downloader");
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
