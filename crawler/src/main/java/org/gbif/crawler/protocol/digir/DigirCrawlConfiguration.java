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
package org.gbif.crawler.protocol.digir;

import org.gbif.crawler.CrawlConfiguration;

import java.net.URI;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.concurrent.ThreadSafe;

import lombok.ToString;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Holds information necessary to crawl a DiGIR resource:
 *
 * <p>
 *
 * <ul>
 *   <li>target URL
 *   <li>resource code
 *   <li>a flag indicating if this is a MANIS endpoint
 * </ul>
 *
 * <p>This object is immutable.
 */
@ThreadSafe
@ToString(callSuper = true)
public class DigirCrawlConfiguration extends CrawlConfiguration {

  private final String resourceCode;

  private final boolean manis;

  public DigirCrawlConfiguration(
      UUID datasetKey, int attempt, URI url, String resourceCode, boolean manis) {
    super(datasetKey, url, attempt);

    this.resourceCode = checkNotNull(resourceCode, "resourceCode can't be null");
    checkArgument(!resourceCode.isEmpty(), "resourceCode can't be empty");

    this.manis = manis;
  }

  public String getResourceCode() {
    return resourceCode;
  }

  public boolean isManis() {
    return manis;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof DigirCrawlConfiguration)) {
      return false;
    }

    final DigirCrawlConfiguration other = (DigirCrawlConfiguration) obj;
    return super.equals(other)
        && Objects.equals(this.resourceCode, other.resourceCode)
        && Objects.equals(this.manis, other.manis);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), resourceCode, manis);
  }

}
