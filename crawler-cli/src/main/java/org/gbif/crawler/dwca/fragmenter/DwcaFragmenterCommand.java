/*
 * Copyright ${today.currentYear} Global Biodiversity Information Facility (GBIF)
 *
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
package org.gbif.crawler.dwca.fragmenter;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

/**
 * This will start a tool that listens to messages about
 */
@MetaInfServices(Command.class)
public class DwcaFragmenterCommand extends ServiceCommand {

  private final DwcaFragmenterConfiguration configuration = new DwcaFragmenterConfiguration();

  public DwcaFragmenterCommand() {
    super("dwcafragmenter");
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

  @Override
  protected Service getService() {
    return new DwcaFragmenterService(configuration);
  }

}
