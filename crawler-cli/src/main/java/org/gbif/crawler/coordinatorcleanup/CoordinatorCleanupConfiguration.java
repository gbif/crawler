/*
 * Copyright 2013 Global Biodiversity Information Facility (GBIF)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.coordinatorcleanup;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;
import org.gbif.cli.PropertyName;
import org.gbif.crawler.common.RegistryConfiguration;
import org.gbif.crawler.common.ZooKeeperConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class CoordinatorCleanupConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

  @Parameter(names = "--ws-url", description = "URL where the crawler WS is running")
  @NotNull
  @PropertyName("crawler.ws.url")
  public String wsUrl;

  @ParametersDelegate
  @NotNull
  @Valid
  public RegistryConfiguration registry = new RegistryConfiguration();

  @Parameter(names = "--interval", description = "Interval in minutes in which this process should run")
  @Min(1)
  public int interval = 1;

  @Parameter(names = "--http-timeout", description = "Timeout from HTTP REST calls, milliseconds")
  @Min(1 * 1000)
  @PropertyName("httpTimeout")
  public int httpTimeout = 3 * 60 * 1000;

  @Parameter(names = "--pipelines-only-mode", description = "Ignore message-based processing steps")
  @PropertyName("pipelinesOnlyMode")
  public boolean pipelinesOnlyMode = false;

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("zooKeeper", zooKeeper)
      .add("interval", interval).add("wsUrl", wsUrl).add("httpTimeout", httpTimeout).toString();
  }
}
