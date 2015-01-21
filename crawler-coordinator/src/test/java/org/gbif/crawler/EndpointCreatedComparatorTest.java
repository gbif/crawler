package org.gbif.crawler;

import org.gbif.api.model.registry.Endpoint;

import java.util.Date;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class EndpointCreatedComparatorTest {

  private static final EndpointCreatedComparator COMP = EndpointCreatedComparator.INSTANCE;
  private final long now = System.currentTimeMillis();

  @Test
  public void testSuccessfulSimpleComparisons() {
    Endpoint e1 = new Endpoint();
    Endpoint e2 = new Endpoint();

    e1.setCreated(new Date(now));
    e2.setCreated(new Date(now));
    assertThat(COMP.compare(e1, e2), equalTo(0));

    e2.setCreated(new Date(now-10000));
    assertThat(COMP.compare(e1, e2), greaterThan(0));

    e2.setCreated(new Date(now+10000));
    assertThat(COMP.compare(e1, e2), lessThan(0));
  }

  @Test
  public void testNullComparisons() {
    Endpoint e1 = new Endpoint();
    Endpoint e2 = new Endpoint();

    assertThat(COMP.compare(e1, e2), equalTo(0));

    e1.setCreated(new Date(now));
    assertThat(COMP.compare(e1, e2), lessThan(0));

    e2.setCreated(new Date(now-10000));
    assertThat(COMP.compare(e1, e2), greaterThan(0));

    e2.setCreated(new Date(now+10000));
    assertThat(COMP.compare(e1, e2), lessThan(0));
  }


}