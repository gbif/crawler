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
package org.gbif.crawler.protocol.digir;

import org.gbif.crawler.strategy.ScientificNameRangeCrawlContext;

import java.net.URI;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DigirScientificNameRangeRequestHandlerTest {

  private static final String FIRST_URL =
      "http://peabody.research.yale.edu/digir/DiGIR.php?request=%3Crequest+xmlns%3D%22http%3A%2F%2Fdigir.net%2Fschema%2Fprotocol%2F2003%2F1.0%22%0A+++++++++xmlns%3Axsi%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%22%0A+++++++++xmlns%3Adwc%3D%22http%3A%2F%2Fdigir.net%2Fschema%2Fconceptual%2Fdarwin%2F2003%2F1.0%22%0A+++++++++xsi%3AschemaLocation%3D%22http%3A%2F%2Fdigir.net%2Fschema%2Fprotocol%2F2003%2F1.0+http%3A%2F%2Fdigir.sourceforge.net%2Fschema%2Fprotocol%2F2003%2F1.0%2Fdigir.xsd+http%3A%2F%2Fdigir.net%2Fschema%2Fconceptual%2Fdarwin%2F2003%2F1.0+http%3A%2F%2Fbnhm.berkeley.edu%2Fmanis%2FDwC%2Fdarwin2jrw030315.xsd%22%3E%0A++%3Cheader%3E%0A++++%3Cversion%3E1.0.0%3C%2Fversion%3E%0A++++%3C%3E%0A++++%3Csource%3EGBIF+Crawler%3C%2Fsource%3E%0A++++%3Cdestination+resource%3D%22ent%22%3Ehttp%3A%2F%2Fpeabody.research.yale.edu%2Fdigir%2FDiGIR.php%3C%2Fdestination%3E%0A++++%3Ctype%3Esearch%3C%2Ftype%3E%0A++%3C%2Fheader%3E%0A++%3Csearch%3E%0A++++%3Cfilter%3E%0A++++++++%3ClessThan%3E%0A++++++++++%3Cdwc%3AScientificName%3EAaa%3C%2Fdwc%3AScientificName%3E%0A++++++++%3C%2FlessThan%3E%0A++++%3C%2Ffilter%3E%0A++++%3Crecords+limit%3D%221000%22+start%3D%220%22%3E%0A++++++%3Cstructure+schemaLocation%3D%22http%3A%2F%2Fbnhm.berkeley.edu%2Fmanis%2FDwC%2Fdarwin2resultfull.xsd%22%2F%3E%0A++++%3C%2Frecords%3E%0A++++%3Ccount%3Efalse%3C%2Fcount%3E%0A++%3C%2Fsearch%3E%0A%3C%2Frequest%3E%0A";

  private static final String SECOND_URL =
      "http://peabody.research.yale.edu/digir/DiGIR.php?request=%3Crequest+xmlns%3D%22http%3A%2F%2Fdigir.net%2Fschema%2Fprotocol%2F2003%2F1.0%22%0A+++++++++xmlns%3Axsi%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%22%0A+++++++++xmlns%3Adwc%3D%22http%3A%2F%2Fdigir.net%2Fschema%2Fconceptual%2Fdarwin%2F2003%2F1.0%22%0A+++++++++xsi%3AschemaLocation%3D%22http%3A%2F%2Fdigir.net%2Fschema%2Fprotocol%2F2003%2F1.0+http%3A%2F%2Fdigir.sourceforge.net%2Fschema%2Fprotocol%2F2003%2F1.0%2Fdigir.xsd+http%3A%2F%2Fdigir.net%2Fschema%2Fconceptual%2Fdarwin%2F2003%2F1.0+http%3A%2F%2Fbnhm.berkeley.edu%2Fmanis%2FDwC%2Fdarwin2jrw030315.xsd%22%3E%0A++%3Cheader%3E%0A++++%3Cversion%3E1.0.0%3C%2Fversion%3E%0A++++%3C%3E%0A++++%3Csource%3EGBIF+Crawler%3C%2Fsource%3E%0A++++%3Cdestination+resource%3D%22ent%22%3Ehttp%3A%2F%2Fpeabody.research.yale.edu%2Fdigir%2FDiGIR.php%3C%2Fdestination%3E%0A++++%3Ctype%3Esearch%3C%2Ftype%3E%0A++%3C%2Fheader%3E%0A++%3Csearch%3E%0A++++%3Cfilter%3E%0A++++++%3Cand%3E%0A++++++++%3CgreaterThanOrEquals%3E%0A++++++++++%3Cdwc%3AScientificName%3EAaa%3C%2Fdwc%3AScientificName%3E%0A++++++++%3C%2FgreaterThanOrEquals%3E%0A++++++++%3ClessThan%3E%0A++++++++++%3Cdwc%3AScientificName%3EAba%3C%2Fdwc%3AScientificName%3E%0A++++++++%3C%2FlessThan%3E%0A++++++%3C%2Fand%3E%0A++++%3C%2Ffilter%3E%0A++++%3Crecords+limit%3D%221000%22+start%3D%220%22%3E%0A++++++%3Cstructure+schemaLocation%3D%22http%3A%2F%2Fbnhm.berkeley.edu%2Fmanis%2FDwC%2Fdarwin2resultfull.xsd%22%2F%3E%0A++++%3C%2Frecords%3E%0A++++%3Ccount%3Efalse%3C%2Fcount%3E%0A++%3C%2Fsearch%3E%0A%3C%2Frequest%3E%0A";

  private static final String LAST_URL =
      "http://peabody.research.yale.edu/digir/DiGIR.php?request=%3Crequest+xmlns%3D%22http%3A%2F%2Fdigir.net%2Fschema%2Fprotocol%2F2003%2F1.0%22%0A+++++++++xmlns%3Axsi%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%22%0A+++++++++xmlns%3Adwc%3D%22http%3A%2F%2Fdigir.net%2Fschema%2Fconceptual%2Fdarwin%2F2003%2F1.0%22%0A+++++++++xsi%3AschemaLocation%3D%22http%3A%2F%2Fdigir.net%2Fschema%2Fprotocol%2F2003%2F1.0+http%3A%2F%2Fdigir.sourceforge.net%2Fschema%2Fprotocol%2F2003%2F1.0%2Fdigir.xsd+http%3A%2F%2Fdigir.net%2Fschema%2Fconceptual%2Fdarwin%2F2003%2F1.0+http%3A%2F%2Fbnhm.berkeley.edu%2Fmanis%2FDwC%2Fdarwin2jrw030315.xsd%22%3E%0A++%3Cheader%3E%0A++++%3Cversion%3E1.0.0%3C%2Fversion%3E%0A++++%3C%3E%0A++++%3Csource%3EGBIF+Crawler%3C%2Fsource%3E%0A++++%3Cdestination+resource%3D%22ent%22%3Ehttp%3A%2F%2Fpeabody.research.yale.edu%2Fdigir%2FDiGIR.php%3C%2Fdestination%3E%0A++++%3Ctype%3Esearch%3C%2Ftype%3E%0A++%3C%2Fheader%3E%0A++%3Csearch%3E%0A++++%3Cfilter%3E%0A++++++++%3CgreaterThanOrEquals%3E%0A++++++++++%3Cdwc%3AScientificName%3EZza%3C%2Fdwc%3AScientificName%3E%0A++++++++%3C%2FgreaterThanOrEquals%3E%0A++++%3C%2Ffilter%3E%0A++++%3Crecords+limit%3D%221000%22+start%3D%221000%22%3E%0A++++++%3Cstructure+schemaLocation%3D%22http%3A%2F%2Fbnhm.berkeley.edu%2Fmanis%2FDwC%2Fdarwin2resultfull.xsd%22%2F%3E%0A++++%3C%2Frecords%3E%0A++++%3Ccount%3Efalse%3C%2Fcount%3E%0A++%3C%2Fsearch%3E%0A%3C%2Frequest%3E%0A";

  @Test
  public void testUrlBuilding() {
    URI targetUrl = URI.create("http://peabody.research.yale.edu/digir/DiGIR.php");
    DigirCrawlConfiguration job =
        new DigirCrawlConfiguration(UUID.randomUUID(), 1, targetUrl, "ent", true);
    DigirScientificNameRangeRequestHandler handler =
        new DigirScientificNameRangeRequestHandler(job);

    ScientificNameRangeCrawlContext context = new ScientificNameRangeCrawlContext();
    String request = handler.buildRequestUrl(context);
    request = request.replaceAll("sendTime.*sendTime", "");
    assertEquals(FIRST_URL, request);

    context.setLowerBound("Aaa");
    context.setUpperBound("Aba");
    request = handler.buildRequestUrl(context);
    request = request.replaceAll("sendTime.*sendTime", "");
    assertEquals(SECOND_URL, request);

    context.setLowerBound("Zza");
    context.setUpperBoundAbsent();
    context.setOffset(1000);
    request = handler.buildRequestUrl(context);
    System.out.println(request);
    request = request.replaceAll("sendTime.*sendTime", "");
    assertEquals(LAST_URL, request);

    // Null jobs are not allowed
    assertThrows(Exception.class, () -> handler.buildRequestUrl(null));
  }

  @Test
  public void testConstructor() {
    assertThrows(Exception.class, () -> new DigirScientificNameRangeRequestHandler(null));
  }
}
