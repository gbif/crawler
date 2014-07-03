package org.gbif.crawler.emlpusher;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This command iterates over all datasets in the crawling dwca directory and inserts any EML files found into the registry.
 */
@MetaInfServices(Command.class)
public class PushEmlCommand extends BaseCommand {

  private static final Logger LOG = LoggerFactory.getLogger(PushEmlCommand.class);
  private final PushEmlConfiguration config = new PushEmlConfiguration();

  public PushEmlCommand() {
    super("pusheml");
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  @Override
  protected void doRun() {
    EmlPusher pusher = EmlPusher.build(config);
    pusher.pushAll();
  }

}
