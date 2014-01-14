package org.gbif.crawler.protocol.digir;

import org.gbif.crawler.CrawlConfiguration;

import java.net.URI;
import java.util.UUID;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Holds information necessary to crawl a DiGIR resource:
 * <p/>
 * <ul>
 * <li>target URL</li>
 * <li>resource code</li>
 * <li>a flag indicating if this is a MANIS endpoint</li>
 * </ul>
 * <p/>
 * This object is immutable.
 */
@ThreadSafe
public class DigirCrawlConfiguration extends CrawlConfiguration {

  private final String resourceCode;

  private final boolean manis;

  public DigirCrawlConfiguration(UUID datasetKey, int attempt, URI url, String resourceCode, boolean manis) {
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
           && Objects.equal(this.resourceCode, other.resourceCode)
           && Objects.equal(this.manis, other.manis);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), resourceCode, manis);
  }

  @Override
  public String toString() {
    return toStringHelper().add("resourceCode", resourceCode).add("manis", manis).toString();
  }

}
