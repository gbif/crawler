package org.gbif.crawler.protocol.tapir;

import org.gbif.crawler.CrawlConfiguration;

import java.net.URI;
import java.util.UUID;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Holds information necessary to crawl a TAPIR resource:
 * <p/>
 * <ul>
 * <li>target URL</li>
 * <li>content namespace, a list of supported namespaces is available in the {@link #VALID_SCHEMAS} field</li>
 * </ul>
 * <p/>
 * This object is immutable.
 */
@ThreadSafe
public class TapirCrawlConfiguration extends CrawlConfiguration {

  public static final ImmutableList<String> VALID_SCHEMAS = ImmutableList.of(
    "http://rs.tdwg.org/dwc/dwcore/",
    "http://rs.tdwg.org/dwc/geospatial/",
    "http://rs.tdwg.org/dwc/curatorial/",
    "http://rs.tdwg.org/dwc/terms/",
    "http://digir.net/schema/conceptual/darwin/2003/1.0",
    "http://www.tdwg.org/schemas/abcd/1.2",
    "http://www.tdwg.org/schemas/abcd/2.05",
    "http://www.tdwg.org/schemas/abcd/2.06"
  );

  private final String contentNamespace;

  public TapirCrawlConfiguration(UUID datasetKey, int attempt, URI url, String contentNamespace) {
    super(datasetKey, url, attempt);

    this.contentNamespace = checkNotNull(contentNamespace, "contentNamespace can't be null");

    checkArgument(VALID_SCHEMAS.contains(contentNamespace),
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
    return super.equals(other)
           && Objects.equal(this.contentNamespace, other.contentNamespace);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), contentNamespace);
  }

  @Override
  public String toString() {
    return toStringHelper().add("contentNamespace", contentNamespace).toString();
  }

}
