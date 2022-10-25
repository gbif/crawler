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
package org.gbif.crawler.protocol.biocase;

import org.gbif.crawler.CrawlConfiguration;

import java.net.URI;
import java.util.UUID;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Holds information necessary to crawl a BioCASe resource:
 *
 * <p>
 *
 * <ul>
 *   <li>target URL
 *   <li>content namespace (only ABCD 1.2 and 2.06 are supported)
 *   <li>dataset title
 * </ul>
 *
 * <p>This object is immutable.
 */
@ThreadSafe
public class BiocaseCrawlConfiguration extends CrawlConfiguration {

  public static final ImmutableList<String> VALID_SCHEMAS =
      ImmutableList.of(
          "http://www.tdwg.org/schemas/abcd/1.2", "http://www.tdwg.org/schemas/abcd/2.06");

  private final String contentNamespace;

  private final String datasetTitle;

  public BiocaseCrawlConfiguration(
      UUID datasetKey, int attempt, URI url, String contentNamespace, String datasetTitle) {
    super(datasetKey, url, attempt);

    this.contentNamespace = checkNotNull(contentNamespace, "contentNamespace can't be null");
    checkArgument(
        VALID_SCHEMAS.contains(contentNamespace),
        "contentNamespace: Only ABCD 1.2 and 2.06 are supported");

    this.datasetTitle = checkNotNull(datasetTitle, "datasetTitle can't be null");
    checkArgument(!datasetTitle.isEmpty(), "datasetTitle can't be empty");
  }

  public String getContentNamespace() {
    return contentNamespace;
  }

  public String getDatasetTitle() {
    return datasetTitle;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof BiocaseCrawlConfiguration)) {
      return false;
    }

    BiocaseCrawlConfiguration other = (BiocaseCrawlConfiguration) obj;
    return super.equals(other)
        && Objects.equal(this.contentNamespace, other.contentNamespace)
        && Objects.equal(this.datasetTitle, other.datasetTitle);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), contentNamespace, datasetTitle);
  }

  @Override
  public String toString() {
    return toStringHelper()
        .add("contentNamespace", contentNamespace)
        .add("datasetTitle", datasetTitle)
        .toString();
  }
}
