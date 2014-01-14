package org.gbif.crawler.strategy;

import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class ScientificNameRangeCrawlJobTest {

  @Test
  public void testDefaultConstructor() {
    ScientificNameRangeCrawlContext job = new ScientificNameRangeCrawlContext();

    assertThat(job.getLowerBound().isPresent()).isFalse();
    assertThat(job.getUpperBound().get()).isEqualTo("Aaa");
    assertThat(job.getOffset()).isZero();
  }

  @Test
  public void testCustomConstructor() {
    ScientificNameRangeCrawlContext job;

    job = new ScientificNameRangeCrawlContext(0, null, null);
    assertThat(job.getLowerBound().isPresent()).isFalse();
    assertThat(job.getUpperBound().isPresent()).isFalse();
    assertThat(job.getOffset()).isZero();

    job = new ScientificNameRangeCrawlContext(0, "aaa", null);
    assertThat(job.getLowerBound().isPresent()).isTrue();
    assertThat(job.getLowerBound().get()).isEqualTo("aaa");
    assertThat(job.getUpperBound().isPresent()).isFalse();
    assertThat(job.getOffset()).isZero();

    job = new ScientificNameRangeCrawlContext(10, "aaa", "zzz");
    assertThat(job.getLowerBound().isPresent()).isTrue();
    assertThat(job.getLowerBound().get()).isEqualTo("aaa");
    assertThat(job.getUpperBound().isPresent()).isTrue();
    assertThat(job.getUpperBound().get()).isEqualTo("zzz");
    assertThat(job.getOffset()).isEqualTo(10);

    try {
      new ScientificNameRangeCrawlContext(0, "", null);
      fail();
    } catch (IllegalArgumentException e) {
    }

    try {
      new ScientificNameRangeCrawlContext(-10, null, null);
      fail();
    } catch (IllegalArgumentException e) {
    }

    try {
      new ScientificNameRangeCrawlContext(0, null, "abcd");
      fail();
    } catch (IllegalArgumentException e) {
    }

  }

  @Test
  public void testSetAbsent() {
    ScientificNameRangeCrawlContext job = new ScientificNameRangeCrawlContext(0, "aaa", "zzz");

    job.setLowerBoundAbsent();
    assertThat(job.getLowerBound().isPresent()).isFalse();
    assertThat(job.getUpperBound().isPresent()).isTrue();

    job.setUpperBoundAbsent();
    assertThat(job.getLowerBound().isPresent()).isFalse();
    assertThat(job.getUpperBound().isPresent()).isFalse();
  }
}
