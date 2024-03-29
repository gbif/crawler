/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.emlpusher;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This command iterates over all datasets in the crawling dwca directory and inserts any EML files
 * found into the registry.
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
