package org.gbif.crawler.strategy;

import java.util.NoSuchElementException;

import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class ScientificNameRangeStrategyTest {

  @Test
  public void testDefaultJob() {
    ScientificNameRangeStrategy strategy = new ScientificNameRangeStrategy(new ScientificNameRangeCrawlContext());
    ScientificNameRangeCrawlContext next = strategy.next();

    // Test if null...Aaa works
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isFalse();
    assertThat(next.getUpperBound().isPresent()).isTrue();
    assertThat(next.getUpperBound().get()).isEqualTo("Aaa");

    // Aaa...Aba
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isTrue();
    assertThat(next.getLowerBound().get()).isEqualTo("Aaa");
    assertThat(next.getUpperBound().isPresent()).isTrue();
    assertThat(next.getUpperBound().get()).isEqualTo("Aba");

    // Aza...Baa
    next.setLowerBound("Aya");
    next.setUpperBound("Aza");
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isTrue();
    assertThat(next.getLowerBound().get()).isEqualTo("Aza");
    assertThat(next.getUpperBound().isPresent()).isTrue();
    assertThat(next.getUpperBound().get()).isEqualTo("Baa");

    // Zxa...Zya, Zya...Zza
    next.setLowerBound("Zxa");
    next.setUpperBound("Zya");
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isTrue();
    assertThat(next.getLowerBound().get()).isEqualTo("Zya");
    assertThat(next.getUpperBound().isPresent()).isTrue();
    assertThat(next.getUpperBound().get()).isEqualTo("Zza");

    // Zza...null
    assertThat(strategy.hasNext()).isTrue();
    next = strategy.next();
    assertThat(next.getOffset()).isZero();
    assertThat(next.getLowerBound().isPresent()).isTrue();
    assertThat(next.getLowerBound().get()).isEqualTo("Zza");
    assertThat(next.getUpperBound().isPresent()).isFalse();
  }

  @Test
  public void testFailure() {
    ScientificNameRangeStrategy strategy = new ScientificNameRangeStrategy(new ScientificNameRangeCrawlContext());
    ScientificNameRangeCrawlContext next = strategy.next();

    next.setLowerBound("Zza");
    next.setUpperBoundAbsent();
    // Make sure there are no more
    assertThat(strategy.hasNext()).isFalse();

    try {
      strategy.next();
      fail();
    } catch (NoSuchElementException e) {
    }
  }

}
