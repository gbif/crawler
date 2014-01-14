package org.gbif.crawler.protocol.biocase;

import org.gbif.crawler.protocol.AbstractScientificNameRangeRequestHandler;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.ImmutableMap;

/**
 * This is a request handler for BioCASe queries using a range of scientific names.
 * <p/>
 * It sends search requests using BioCASe protocol version 1.3 and supports ABCD in version 1.2 and 2.06 and requests
 * 1000 results per page by default.
 * <p/>
 * This class is not thread-safe.
 */
@NotThreadSafe
public class BiocaseScientificNameRangeRequestHandler extends AbstractScientificNameRangeRequestHandler {

  private static final String TEMPLATE_LOCATION = "template/biocase/scientificNameRangeSearch.ftl";
  private static final String REQUEST_PARAM_KEY = "request";

  private static final int MAX_RESULTS = 1000;

  private static final String TITLE_KEY = "titlePath";
  private static final String SUBJECT_KEY = "subjectPath";

  private static final ImmutableMap<String, ImmutableMap<String, String>> SCHEMA_INFOS;

  static {
    ImmutableMap<String, String> abcd12 = ImmutableMap.<String, String>builder()
      .put(TITLE_KEY, "/DataSets/DataSet/OriginalSource/SourceName")
      .put(SUBJECT_KEY,
        "/DataSets/DataSet/Units/Unit/Identifications/Identification/TaxonIdentified/NameAuthorYearString")
      .build();

    ImmutableMap<String, String> abcd206 = ImmutableMap.<String, String>builder()
      .put(TITLE_KEY, "/DataSets/DataSet/Metadata/Description/Representation/Title")
      .put(SUBJECT_KEY,
        "/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/ScientificName/FullScientificNameString")
      .build();

    SCHEMA_INFOS = ImmutableMap.<String, ImmutableMap<String, String>>builder()
      .put("http://www.tdwg.org/schemas/abcd/1.2", abcd12)
      .put("http://www.tdwg.org/schemas/abcd/2.06", abcd206)
      .build();
  }

  private final ImmutableMap<String, Object> defaultContext;

  /**
   * Initializes this request handler.
   */
  public BiocaseScientificNameRangeRequestHandler(BiocaseCrawlConfiguration configuration) {
    super(configuration.getUrl(), TEMPLATE_LOCATION, REQUEST_PARAM_KEY);

    defaultContext = ImmutableMap.<String, Object>builder()
      .put("limit", Integer.toString(MAX_RESULTS))
      .put("contentNamespace", configuration.getContentNamespace())
      .put("datasetTitle", configuration.getDatasetTitle())
      .put(TITLE_KEY, SCHEMA_INFOS.get(configuration.getContentNamespace()).get(TITLE_KEY))
      .put(SUBJECT_KEY, SCHEMA_INFOS.get(configuration.getContentNamespace()).get(SUBJECT_KEY))
      .build();
  }

  @Override
  protected ImmutableMap<String, Object> getDefaultContext() {
    return defaultContext;
  }

  @Override
  public int getLimit() {
    return MAX_RESULTS;
  }
}
