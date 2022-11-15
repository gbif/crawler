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
