/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
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
package org.gbif.crawler.dwca.downloader;

import org.gbif.cli.PropertyName;
import org.gbif.crawler.common.crawlserver.CrawlServerConfiguration;

import java.io.File;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.google.common.base.Objects;

public class DownloaderConfiguration extends CrawlServerConfiguration {

  @Parameter(names = "--archive-repository")
  @NotNull
  public File archiveRepository;

  // High 30 minute default is to cope with a very slow DWCA webservice.
  @Parameter(names = "--http-timeout", description = "Timeout for HTTP calls, milliseconds")
  @Min(1 * 1000)
  @PropertyName("httpTimeout")
  public int httpTimeout = 30 * 60 * 1000;

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("super", super.toString())
        .add("archiveRepository", archiveRepository)
        .toString();
  }
}
