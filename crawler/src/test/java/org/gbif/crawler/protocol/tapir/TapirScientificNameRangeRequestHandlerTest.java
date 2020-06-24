/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
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

import org.gbif.crawler.strategy.ScientificNameRangeCrawlContext;

import java.net.URI;
import java.util.UUID;

import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class TapirScientificNameRangeRequestHandlerTest {

  private static final String FIRST_URL =
      "http://mockhost1.gbif.org/tapirlink/tapir.php/pontaurus?limit=1000&upper=Aaa&lower&t=http%3A%2F%2Frs.gbif.org%2Ftemplates%2Ftapir%2Fdwc%2F1.4%2Fsci_name_range.xml&op=s&start=0";
  private static final String SECOND_URL =
      "http://mockhost1.gbif.org/tapirlink/tapir.php/pontaurus?limit=1000&upper=Aba&lower=Aaa&t=http%3A%2F%2Frs.gbif.org%2Ftemplates%2Ftapir%2Fdwc%2F1.4%2Fsci_name_range.xml&op=s&start=0";
  private static final String LAST_URL =
      "http://mockhost1.gbif.org/tapirlink/tapir.php/pontaurus?limit=1000&upper&lower=Zza&t=http%3A%2F%2Frs.gbif.org%2Ftemplates%2Ftapir%2Fdwc%2F1.4%2Fsci_name_range.xml&op=s&start=1000";

  @Test
  public void testUrlBuilding() {
    URI targetUrl = URI.create("http://mockhost1.gbif.org/tapirlink/tapir.php/pontaurus");
    TapirCrawlConfiguration job =
        new TapirCrawlConfiguration(
            UUID.randomUUID(), 1, targetUrl, "http://rs.tdwg.org/dwc/dwcore/");
    TapirScientificNameRangeRequestHandler handler =
        new TapirScientificNameRangeRequestHandler(job);

    ScientificNameRangeCrawlContext context = new ScientificNameRangeCrawlContext();

    assertThat(handler.buildRequestUrl(context)).isEqualTo(FIRST_URL);
    context.setLowerBound("Aaa");
    context.setUpperBound("Aba");
    assertThat(handler.buildRequestUrl(context)).isEqualTo(SECOND_URL);

    context.setLowerBound("Zza");
    context.setUpperBoundAbsent();
    context.setOffset(1000);
    assertThat(handler.buildRequestUrl(context)).isEqualTo(LAST_URL);

    // Null jobs are not allowed
    try {
      handler.buildRequestUrl(null);
      fail();
    } catch (Exception e) {

    }
  }

  @Test
  public void testConstructor() {
    try {
      new TapirScientificNameRangeRequestHandler(null);
      fail();
    } catch (Exception e) {
    }
  }
}
