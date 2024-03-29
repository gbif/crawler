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
package org.gbif.crawler.protocol.biocase;

import org.gbif.crawler.strategy.ScientificNameRangeCrawlContext;

import java.net.URI;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BiocaseScientificNameRangeRequestHandlerTest {

  private static final String FIRST_URL =
      "http://mockhost1.gbif.org/biocase/pywrapper.cgi?dsa=pontaurus&request=%3C%3Fxml+version%3D%271.0%27+encoding%3D%27UTF-8%27%3F%3E%0A%3Crequest+xmlns%3D%27http%3A%2F%2Fwww.biocase.org%2Fschemas%2Fprotocol%2F1.3%27%0A+++++++++xmlns%3Axsi%3D%27http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%27%0A+++++++++xsi%3AschemaLocation%3D%27http%3A%2F%2Fwww.biocase.org%2Fschemas%2Fprotocol%2F1.3+http%3A%2F%2Fwww.bgbm.org%2Fbiodivinf%2FSchema%2Fprotocol_1_3.xsd%27%3E%0A++%3Cheader%3E%0A++++%3Ctype%3Esearch%3C%2Ftype%3E%0A++%3C%2Fheader%3E%0A++%3Csearch%3E%0A++++%3CrequestFormat%3Ehttp%3A%2F%2Fwww.tdwg.org%2Fschemas%2Fabcd%2F2.06%3C%2FrequestFormat%3E%0A++++%3CresponseFormat+start%3D%220%22+limit%3D%221000%22%3Ehttp%3A%2F%2Fwww.tdwg.org%2Fschemas%2Fabcd%2F2.06%3C%2FresponseFormat%3E%0A++++%3Cfilter%3E%0A++++++%3Cand%3E%0A++++++++%3Cequals+path%3D%22%2FDataSets%2FDataSet%2FMetadata%2FDescription%2FRepresentation%2FTitle%22%3EPontaurus%3C%2Fequals%3E%0A++++++++%3Cand%3E%0A++++++++++%3ClessThan+path%3D%22%2FDataSets%2FDataSet%2FUnits%2FUnit%2FIdentifications%2FIdentification%2FResult%2FTaxonIdentified%2FScientificName%2FFullScientificNameString%22%3EAaa%3C%2FlessThan%3E%0A++++++++%3C%2Fand%3E%0A++++++%3C%2Fand%3E%0A++++%3C%2Ffilter%3E%0A++++%3Ccount%3Efalse%3C%2Fcount%3E%0A++%3C%2Fsearch%3E%0A%3C%2Frequest%3E%0A";

  private static final String SECOND_URL =
      "http://mockhost1.gbif.org/biocase/pywrapper.cgi?dsa=pontaurus&request=%3C%3Fxml+version%3D%271.0%27+encoding%3D%27UTF-8%27%3F%3E%0A%3Crequest+xmlns%3D%27http%3A%2F%2Fwww.biocase.org%2Fschemas%2Fprotocol%2F1.3%27%0A+++++++++xmlns%3Axsi%3D%27http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%27%0A+++++++++xsi%3AschemaLocation%3D%27http%3A%2F%2Fwww.biocase.org%2Fschemas%2Fprotocol%2F1.3+http%3A%2F%2Fwww.bgbm.org%2Fbiodivinf%2FSchema%2Fprotocol_1_3.xsd%27%3E%0A++%3Cheader%3E%0A++++%3Ctype%3Esearch%3C%2Ftype%3E%0A++%3C%2Fheader%3E%0A++%3Csearch%3E%0A++++%3CrequestFormat%3Ehttp%3A%2F%2Fwww.tdwg.org%2Fschemas%2Fabcd%2F2.06%3C%2FrequestFormat%3E%0A++++%3CresponseFormat+start%3D%220%22+limit%3D%221000%22%3Ehttp%3A%2F%2Fwww.tdwg.org%2Fschemas%2Fabcd%2F2.06%3C%2FresponseFormat%3E%0A++++%3Cfilter%3E%0A++++++%3Cand%3E%0A++++++++%3Cequals+path%3D%22%2FDataSets%2FDataSet%2FMetadata%2FDescription%2FRepresentation%2FTitle%22%3EPontaurus%3C%2Fequals%3E%0A++++++++%3Cand%3E%0A++++++++++%3CgreaterThanOrEquals+path%3D%22%2FDataSets%2FDataSet%2FUnits%2FUnit%2FIdentifications%2FIdentification%2FResult%2FTaxonIdentified%2FScientificName%2FFullScientificNameString%22%3EAaa%3C%2FgreaterThanOrEquals%3E%0A++++++++++%3ClessThan+path%3D%22%2FDataSets%2FDataSet%2FUnits%2FUnit%2FIdentifications%2FIdentification%2FResult%2FTaxonIdentified%2FScientificName%2FFullScientificNameString%22%3EAba%3C%2FlessThan%3E%0A++++++++%3C%2Fand%3E%0A++++++%3C%2Fand%3E%0A++++%3C%2Ffilter%3E%0A++++%3Ccount%3Efalse%3C%2Fcount%3E%0A++%3C%2Fsearch%3E%0A%3C%2Frequest%3E%0A";

  private static final String THIRD_URL =
      "http://mockhost1.gbif.org/biocase/pywrapper.cgi?dsa=pontaurus&request=%3C%3Fxml+version%3D%271.0%27+encoding%3D%27UTF-8%27%3F%3E%0A%3Crequest+xmlns%3D%27http%3A%2F%2Fwww.biocase.org%2Fschemas%2Fprotocol%2F1.3%27%0A+++++++++xmlns%3Axsi%3D%27http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%27%0A+++++++++xsi%3AschemaLocation%3D%27http%3A%2F%2Fwww.biocase.org%2Fschemas%2Fprotocol%2F1.3+http%3A%2F%2Fwww.bgbm.org%2Fbiodivinf%2FSchema%2Fprotocol_1_3.xsd%27%3E%0A++%3Cheader%3E%0A++++%3Ctype%3Esearch%3C%2Ftype%3E%0A++%3C%2Fheader%3E%0A++%3Csearch%3E%0A++++%3CrequestFormat%3Ehttp%3A%2F%2Fwww.tdwg.org%2Fschemas%2Fabcd%2F2.06%3C%2FrequestFormat%3E%0A++++%3CresponseFormat+start%3D%221000%22+limit%3D%221000%22%3Ehttp%3A%2F%2Fwww.tdwg.org%2Fschemas%2Fabcd%2F2.06%3C%2FresponseFormat%3E%0A++++%3Cfilter%3E%0A++++++%3Cand%3E%0A++++++++%3Cequals+path%3D%22%2FDataSets%2FDataSet%2FMetadata%2FDescription%2FRepresentation%2FTitle%22%3EPontaurus%3C%2Fequals%3E%0A++++++++%3Cand%3E%0A++++++++++%3CgreaterThanOrEquals+path%3D%22%2FDataSets%2FDataSet%2FUnits%2FUnit%2FIdentifications%2FIdentification%2FResult%2FTaxonIdentified%2FScientificName%2FFullScientificNameString%22%3EZza%3C%2FgreaterThanOrEquals%3E%0A++++++++%3C%2Fand%3E%0A++++++%3C%2Fand%3E%0A++++%3C%2Ffilter%3E%0A++++%3Ccount%3Efalse%3C%2Fcount%3E%0A++%3C%2Fsearch%3E%0A%3C%2Frequest%3E%0A";

  private static final String LAST_URL =
      "http://mockhost1.gbif.org/biocase/pywrapper.cgi?dsa=pontaurus&request=%3C%3Fxml+version%3D%271.0%27+encoding%3D%27UTF-8%27%3F%3E%0A%3Crequest+xmlns%3D%27http%3A%2F%2Fwww.biocase.org%2Fschemas%2Fprotocol%2F1.3%27%0A+++++++++xmlns%3Axsi%3D%27http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%27%0A+++++++++xsi%3AschemaLocation%3D%27http%3A%2F%2Fwww.biocase.org%2Fschemas%2Fprotocol%2F1.3+http%3A%2F%2Fwww.bgbm.org%2Fbiodivinf%2FSchema%2Fprotocol_1_3.xsd%27%3E%0A++%3Cheader%3E%0A++++%3Ctype%3Esearch%3C%2Ftype%3E%0A++%3C%2Fheader%3E%0A++%3Csearch%3E%0A++++%3CrequestFormat%3Ehttp%3A%2F%2Fwww.tdwg.org%2Fschemas%2Fabcd%2F2.06%3C%2FrequestFormat%3E%0A++++%3CresponseFormat+start%3D%220%22+limit%3D%221000%22%3Ehttp%3A%2F%2Fwww.tdwg.org%2Fschemas%2Fabcd%2F2.06%3C%2FresponseFormat%3E%0A++++%3Cfilter%3E%0A++++++%3Cand%3E%0A++++++++%3Cequals+path%3D%22%2FDataSets%2FDataSet%2FMetadata%2FDescription%2FRepresentation%2FTitle%22%3EPontaurus%3C%2Fequals%3E%0A++++++++%3CisNull+path%3D%22%2FDataSets%2FDataSet%2FUnits%2FUnit%2FIdentifications%2FIdentification%2FResult%2FTaxonIdentified%2FScientificName%2FFullScientificNameString%22%2F%3E%0A++++++%3C%2Fand%3E%0A++++%3C%2Ffilter%3E%0A++++%3Ccount%3Efalse%3C%2Fcount%3E%0A++%3C%2Fsearch%3E%0A%3C%2Frequest%3E%0A";

  @Test
  public void testUrlBuilding() {
    URI targetUrl = URI.create("http://mockhost1.gbif.org/biocase/pywrapper.cgi?dsa=pontaurus");
    BiocaseCrawlConfiguration job =
        new BiocaseCrawlConfiguration(
            UUID.randomUUID(), 1, targetUrl, "http://www.tdwg.org/schemas/abcd/2.06", "Pontaurus");
    BiocaseScientificNameRangeRequestHandler handler =
        new BiocaseScientificNameRangeRequestHandler(job);

    ScientificNameRangeCrawlContext context = new ScientificNameRangeCrawlContext();
    assertEquals(FIRST_URL, handler.buildRequestUrl(context));

    context.setLowerBound("Aaa");
    context.setUpperBound("Aba");
    assertEquals(SECOND_URL, handler.buildRequestUrl(context));

    context.setLowerBound("Zza");
    context.setUpperBoundAbsent();
    context.setOffset(1000);
    assertEquals(THIRD_URL, handler.buildRequestUrl(context));

    context.setLowerBoundAbsent();
    context.setUpperBoundAbsent();
    context.setOffset(0);
    assertEquals(LAST_URL, handler.buildRequestUrl(context));

    // Null jobs are not allowed
    assertThrows(Exception.class, () -> handler.buildRequestUrl(null));
  }

  @Test
  public void testConstructor() {
    assertThrows(Exception.class, () -> new BiocaseScientificNameRangeRequestHandler(null));
  }
}
