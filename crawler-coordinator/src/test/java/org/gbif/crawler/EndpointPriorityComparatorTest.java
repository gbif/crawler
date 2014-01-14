package org.gbif.crawler;

import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.vocabulary.EndpointType;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class EndpointPriorityComparatorTest {

  private static final EndpointPriorityComparator COMP = new EndpointPriorityComparator();

  @Test
  public void testSuccessfulSimpleComparisons() {
    Endpoint e1 = new Endpoint();
    Endpoint e2 = new Endpoint();

    e1.setType(EndpointType.TAPIR);
    e2.setType(EndpointType.BIOCASE);
    assertThat(COMP.compare(e1, e2), greaterThan(0));

    e1.setType(EndpointType.BIOCASE);
    e2.setType(EndpointType.BIOCASE);
    assertThat(COMP.compare(e1, e2), equalTo(0));

    e1.setType(EndpointType.BIOCASE);
    e2.setType(EndpointType.TAPIR);
    assertThat(COMP.compare(e1, e2), lessThan(0));

    e1.setType(EndpointType.BIOCASE);
    e2.setType(EndpointType.DIGIR_MANIS);
    assertThat(COMP.compare(e1, e2), greaterThan(0));

    e1.setType(EndpointType.DWC_ARCHIVE);
    e2.setType(EndpointType.TAPIR);
    assertThat(COMP.compare(e1, e2), greaterThan(0));
  }

  @Test
  public void testPriorityComparisons() {
    Endpoint e1 = new Endpoint();
    Endpoint e2 = new Endpoint();

    e1.setType(EndpointType.TAPIR);
    e2.setType(EndpointType.BIOCASE);
    assertThat(COMP.compare(e1, e2), greaterThan(0));

    e1.setType(EndpointType.DIGIR_MANIS);
    e2.setType(EndpointType.DIGIR_MANIS);
    assertThat(COMP.compare(e1, e2), equalTo(0));
  }

  @Test(expected = ClassCastException.class)
  public void testInvalidComparison1() {
    Endpoint e1 = new Endpoint();
    Endpoint e2 = new Endpoint();

    e1.setType(EndpointType.TAPIR);
    e2.setType(EndpointType.WMS);
    COMP.compare(e1, e2);
  }

  @Test(expected = ClassCastException.class)
  public void testInvalidComparison2() {
    Endpoint e1 = new Endpoint();
    Endpoint e2 = new Endpoint();

    e1.setType(EndpointType.WMS);
    e2.setType(EndpointType.TAPIR);
    COMP.compare(e1, e2);
  }


}
