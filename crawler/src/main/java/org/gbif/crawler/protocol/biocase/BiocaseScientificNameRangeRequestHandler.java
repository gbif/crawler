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

import org.gbif.crawler.protocol.AbstractScientificNameRangeRequestHandler;

import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;


/**
 * This is a request handler for BioCASe queries using a range of scientific names.
 *
 * <p>It sends search requests using BioCASe protocol version 1.3 and supports ABCD in version 1.2
 * and 2.06 and requests 1000 results per page by default.
 *
 * <p>This class is not thread-safe.
 */
@NotThreadSafe
public class BiocaseScientificNameRangeRequestHandler
    extends AbstractScientificNameRangeRequestHandler {

  private static final String TEMPLATE_RANGE_LOCATION =
      "template/biocase/scientificNameRangeSearch.ftl";
  private static final String TEMPLATE_NULL_LOCATION =
      "template/biocase/scientificNameNullSearch.ftl";
  private static final String REQUEST_PARAM_KEY = "request";

  private static final int MAX_RESULTS = 1000;

  private static final String TITLE_KEY = "titlePath";
  private static final String SUBJECT_KEY = "subjectPath";

  private static final Map<String, Map<String, String>> SCHEMA_INFOS;

  static {
    Map<String, String> abcd12 =
        Map.of(TITLE_KEY,
            "/DataSets/DataSet/OriginalSource/SourceName",
                SUBJECT_KEY,
                "/DataSets/DataSet/Units/Unit/Identifications/Identification/TaxonIdentified/NameAuthorYearString");

    Map<String, String> abcd206 =
      Map.of(TITLE_KEY, "/DataSets/DataSet/Metadata/Description/Representation/Title",
             SUBJECT_KEY,
          "/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/ScientificName/FullScientificNameString");

    SCHEMA_INFOS =
      Map.of("http://www.tdwg.org/schemas/abcd/1.2", abcd12,
            "http://www.tdwg.org/schemas/abcd/2.06", abcd206);
  }

  private final Map<String, Object> defaultContext;

  /** Initializes this request handler. */
  public BiocaseScientificNameRangeRequestHandler(BiocaseCrawlConfiguration configuration) {
    super(
        configuration.getUrl(), TEMPLATE_RANGE_LOCATION, TEMPLATE_NULL_LOCATION, REQUEST_PARAM_KEY);

    defaultContext =
        Map.of("limit", Integer.toString(MAX_RESULTS),
            "contentNamespace", configuration.getContentNamespace(),
            "datasetTitle", configuration.getDatasetTitle(),
            TITLE_KEY, SCHEMA_INFOS.get(configuration.getContentNamespace()).get(TITLE_KEY),
            SUBJECT_KEY, SCHEMA_INFOS.get(configuration.getContentNamespace()).get(SUBJECT_KEY));
  }

  @Override
  protected Map<String, Object> getDefaultContext() {
    return defaultContext;
  }

  @Override
  public int getLimit() {
    return MAX_RESULTS;
  }
}
