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

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.RegistryConfiguration;
import org.gbif.crawler.common.ZooKeeperConfiguration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.ToString;

@ToString
public class CoordinatorConfiguration {

  @ParametersDelegate @Valid @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate @Valid @NotNull
  public ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

  @ParametersDelegate @Valid @NotNull
  public RegistryConfiguration registry = new RegistryConfiguration();

  @Parameter(names = "--pool-size")
  @Min(1)
  public int poolSize = 5;

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName = "crawler_coordinator";

}
