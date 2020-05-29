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
package org.gbif.crawler.abcda;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;
import org.gbif.cli.IgnoreProperty;
import org.gbif.cli.PropertyName;
import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.RegistryConfiguration;
import org.gbif.crawler.common.ZooKeeperConfiguration;
import org.gbif.crawler.common.crawlserver.CrawlServerConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.File;

public class AbcdaConfiguration extends CrawlServerConfiguration {

  public static final String ABCDA_SUFFIX = ".abcda";

  @ParametersDelegate
  @Valid
  @NotNull
  public RegistryConfiguration registry = new RegistryConfiguration();

  @Parameter(names = "--archive-repository")
  @NotNull
  public File archiveRepository;

  @Parameter(names = "--http-timeout", description = "Timeout for HTTP calls, milliseconds")
  @Min(1 * 1000)
  @PropertyName("httpTimeout")
  public int httpTimeout = 10 * 60 * 1000;

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("registry", registry)
      .add("messaging", messaging)
      .add("zooKeeper", zooKeeper)
      .add("archiveRepository", archiveRepository)
      .add("poolSize", poolSize)
      .toString();
  }
}
