package org.gbif.crawler.camtrapdp.converter;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.crawler.camtrapdp.CamtrapDpConfiguration;

import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

/**
 * Entry class for cli command, to start service to convert downloaded CamtrapDP archive to DwC-A.
 * This command starts a service which listens to the {@link
 * org.gbif.common.messaging.api.messages.CamtrapDpDownloadFinishedMessage } and performs the conversion.
 */
@MetaInfServices(Command.class)
public class CamtrapDpToDwcaCommand extends ServiceCommand {

  private final CamtrapDpConfiguration config = new CamtrapDpConfiguration();

  public CamtrapDpToDwcaCommand() {
    super("camtrapdptodwca");
  }

  @Override
  protected Service getService() {
    return new CamtrapDpToDwcaService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
