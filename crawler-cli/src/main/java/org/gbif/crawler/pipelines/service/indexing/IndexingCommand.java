package org.gbif.crawler.pipelines.service.indexing;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.crawler.pipelines.config.IndexingConfiguration;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to index interpreted dataset
 * This command starts a service which listens to the {@link org.gbif.common.messaging.api.messages } and perform conversion
 */
@MetaInfServices(Command.class)
public class IndexingCommand extends ServiceCommand {

  private final IndexingConfiguration config = new IndexingConfiguration();

  public IndexingCommand() {
    super("index-dataset");
  }

  @Override
  protected Service getService() {
    return new IndexingService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}