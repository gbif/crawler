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
package org.gbif.crawler.strategy;

import org.gbif.crawler.CrawlContext;

import java.util.Objects;
import java.util.Optional;

import com.google.common.base.Preconditions;

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.Size;
import lombok.ToString;


/**
 * A context object used by {@link ScientificNameRangeStrategy} to encode lower and upper bounds for
 * scientific name ranges. At the moment either range has to be either absent/null or a three
 * character string.
 */
// TODO: Make sure that lowerBound is "less than" upperBound
// TODO: We should probably just take a valid lower bound and calculate upper bound from there
// because we can't support
//       anything else anyway at the moment
@ToString
public class ScientificNameRangeCrawlContext extends CrawlContext {

  /**
   * Some providers enforce a minimum length of search/filter terms. That's why we enforce it here
   * as well. Three seems to be a common value.
   */
  private static final int MIN_LENGTH = 3;

  @Size(min = MIN_LENGTH, max = MIN_LENGTH)
  private Optional<String> lowerBound = Optional.empty();

  @Size(min = MIN_LENGTH, max = MIN_LENGTH)
  private Optional<String> upperBound = Optional.of("Aaa");

  /** The default is to start at the very beginning which means everything before <em>aaa</em>. */
  public ScientificNameRangeCrawlContext() {}

  public ScientificNameRangeCrawlContext(
      int offset, @Nullable String lowerBound, @Nullable String upperBound) {
    super(offset);
    setUpperBound(upperBound);
    setLowerBound(lowerBound);
  }

  public Optional<String> getLowerBound() {
    return lowerBound;
  }

  public final void setLowerBound(@Nullable String lowerBound) {
    Preconditions.checkArgument(
        lowerBound == null || lowerBound.length() == MIN_LENGTH,
        "Lower bound needs to be either absent/null or three characters long");
    this.lowerBound = Optional.ofNullable(lowerBound);
  }

  public Optional<String> getUpperBound() {
    return upperBound;
  }

  public final void setUpperBound(@Nullable String upperBound) {
    Preconditions.checkArgument(
        upperBound == null || upperBound.length() == MIN_LENGTH,
        "Upper bound needs to be either absent/null or three characters long");
    this.upperBound = Optional.ofNullable(upperBound);
  }

  public void setLowerBoundAbsent() {
    lowerBound = Optional.empty();
  }

  public void setUpperBoundAbsent() {
    upperBound = Optional.empty();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ScientificNameRangeCrawlContext other = (ScientificNameRangeCrawlContext) obj;
    return Objects.equals(this.getOffset(), other.getOffset())
        && Objects.equals(this.lowerBound, other.lowerBound)
        && Objects.equals(this.upperBound, other.upperBound);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getOffset(), lowerBound, upperBound);
  }

}
