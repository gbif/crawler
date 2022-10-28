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
package org.gbif.crawler.scheduler;

import org.gbif.cli.PropertyName;
import org.gbif.common.messaging.config.MessagingConfiguration;

import java.util.List;
import java.util.Map;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


public class CrawlSchedulerConfiguration {

  @Parameter(names = "--crawler-ws", description = "URL where the crawler WS is running")
  @NotNull
  @PropertyName("crawler.ws.url")
  public String crawlerWsUrl;

  @Parameter(names = "--registry-ws")
  @PropertyName("registry.ws.url")
  @NotNull
  public String registryWsUrl = "http://api.gbif.org/";

  @Parameter(
      names = "--interval",
      description = "Interval in minutes in which this process should run")
  @Min(1)
  public int interval = 6000;

  @Parameter(
      names = "--last-crawled",
      description =
          "The number of days that must have expired since the last crawl "
              + "to make a dataset eligible for a recrawl (e.g. 90 would trigger"
              + "crawls of datasets that have not been crawled in the last 90 "
              + "days)")
  @Min(1)
  public int maxLastCrawledInDays = 90;

  @Parameter(
      names = "--max-crawls",
      description = "Number of crawls that we limit to in the crawler (approximate " + "value)")
  @Min(1)
  public int maximumCrawls = 50;

  @ParametersDelegate @Valid @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @DynamicParameter(
      names = "--omissions",
      description =
          "Unique keys of installations, datasets or publishers which will"
              + "be omitted from crawling.  To aid usability, a sensible "
              + "description is given which each key")
  public Map<String, String> omittedKeys = Maps.newHashMap();

  @Parameter(names = "--supportedTypes", description = "Types of dataset that can be crawled")
  public List<String> supportedTypes = Lists.newArrayList();

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("crawlerWsUrl", crawlerWsUrl)
        .add("registryWsUrl", registryWsUrl)
        .add("interval", interval)
        .add("maxLastCrawledInDays", maxLastCrawledInDays)
        .add("maximumCrawls", maximumCrawls)
        .add("messaging", messaging)
        .toString();
  }
}
