package org.gbif.crawler.abcda.downloader;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.crawler.abcda.AbcdaConfiguration;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
public class DownloaderCommand extends ServiceCommand {

  private final AbcdaConfiguration configuration = new AbcdaConfiguration();

  public DownloaderCommand() {
    super("abcdadownloader");
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
