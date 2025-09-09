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
package org.gbif.crawler.dwcdp.downloader;

import com.google.common.util.concurrent.Service;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.crawler.camtrapdp.CamtrapDpConfiguration;

import org.gbif.crawler.dwcdp.DwcDpConfiguration;

import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
public class DownloaderCommand extends ServiceCommand {

  private final DwcDpConfiguration configuration = new DwcDpConfiguration();

  public DownloaderCommand() {
    super("dwcdpdownloader");
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
