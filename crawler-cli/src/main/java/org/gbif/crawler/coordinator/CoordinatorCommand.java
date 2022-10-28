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
package org.gbif.crawler.coordinator;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

@MetaInfServices(Command.class)
public class CoordinatorCommand extends ServiceCommand {

  private final CoordinatorConfiguration config = new CoordinatorConfiguration();

  public CoordinatorCommand() {
    super("coordinator");
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  @Override
  protected Service getService() {
    return new CoordinatorService(config);
  }
}
