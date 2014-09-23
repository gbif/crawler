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
package org.gbif.crawler.xml.crawlserver;

import org.gbif.crawler.common.crawlserver.CrawlServerConfiguration;

import java.io.File;
import javax.validation.constraints.Min;

import com.beust.jcommander.Parameter;
import com.google.common.base.Objects;

@SuppressWarnings("PublicField")
public class XmlCrawlServerConfiguration extends CrawlServerConfiguration {

  @Parameter(names = "--min-lock-delay", description = "An optional minimum delay before releasing a lock on an URL.")
  @Min(1)
  public Long minLockDelay;

  @Parameter(names = "--max-lock-delay", description = "An optional maximum delay before releasing a lock on an URL.")
  @Min(1)
  public Long maxLockDelay;

  @Parameter(names = "--response-archive", description = "An optional directory where all the responses will be saved")
  public File responseArchive;

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("super", super.toString()).add("minLockDelay", minLockDelay)
      .add("maxLockDelay", maxLockDelay).add("responseArchive", responseArchive).toString();
  }

}
