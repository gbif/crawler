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
package org.gbif.crawler.protocol.tapir;

import org.gbif.crawler.CrawlConfiguration;

import java.net.URI;
import java.util.UUID;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import lombok.ToString;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Holds information necessary to crawl a TAPIR resource:
 *
 * <p>
 *
 * <ul>
 *   <li>target URL
 *   <li>content namespace, a list of supported namespaces is available in the {@link
 *       #VALID_SCHEMAS} field
 * </ul>
 *
 * <p>This object is immutable.
 */
@ThreadSafe
@ToString(callSuper = true)
public class TapirCrawlConfiguration extends CrawlConfiguration {

  public static final ImmutableList<String> VALID_SCHEMAS =
      ImmutableList.of(
          "http://rs.tdwg.org/dwc/dwcore/",
          "http://rs.tdwg.org/dwc/geospatial/",
          "http://rs.tdwg.org/dwc/curatorial/",
          "http://rs.tdwg.org/dwc/terms/",
          "http://digir.net/schema/conceptual/darwin/2003/1.0",
          "http://www.tdwg.org/schemas/abcd/1.2",
          "http://www.tdwg.org/schemas/abcd/2.05",
          "http://www.tdwg.org/schemas/abcd/2.06");

  private final String contentNamespace;

  public TapirCrawlConfiguration(UUID datasetKey, int attempt, URI url, String contentNamespace) {
    super(datasetKey, url, attempt);

    this.contentNamespace = checkNotNull(contentNamespace, "contentNamespace can't be null");

    checkArgument(
        VALID_SCHEMAS.contains(contentNamespace),
        "contentNamespace not supported: [" + contentNamespace + "]");
  }

  public String getContentNamespace() {
    return contentNamespace;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TapirCrawlConfiguration)) {
      return false;
    }

    final TapirCrawlConfiguration other = (TapirCrawlConfiguration) obj;
    return super.equals(other) && Objects.equal(this.contentNamespace, other.contentNamespace);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), contentNamespace);
  }

}
