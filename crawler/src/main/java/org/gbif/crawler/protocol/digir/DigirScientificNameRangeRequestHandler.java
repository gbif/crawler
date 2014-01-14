package org.gbif.crawler.protocol.digir;

import org.gbif.crawler.protocol.AbstractScientificNameRangeRequestHandler;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.ImmutableMap;

/**
 * This is a request handler for DiGIR queries using a range of scientific names.
 * <p/>
 * It supports MANIS and normal DiGIR endpoints and requests 1000 results per page by default.
 * <p/>
 * This class is not thread-safe.
 */
@NotThreadSafe
public class DigirScientificNameRangeRequestHandler extends AbstractScientificNameRangeRequestHandler {

  private static final String TEMPLATE_LOCATION = "template/digir/scientificNameRangeSearch.ftl";
  private static final String REQUEST_PARAM_KEY = "request";

  private static final int MAX_RESULTS = 1000;

  private static final String SCHEMA_LOCATION_KEY = "schemaLocation";
  private static final String RECORD_SCHEMA_LOCATION_KEY = "recordSchemaLocation";

  private static final ImmutableMap<Boolean, ImmutableMap<String, String>> SCHEMA_INFOS;

  static {
    ImmutableMap<String, String> normalDigir = ImmutableMap.<String, String>builder()
      .put(SCHEMA_LOCATION_KEY, "http://digir.sourceforge.net/schema/conceptual/darwin/2003/1.0/darwin2.xsd")
      .put(RECORD_SCHEMA_LOCATION_KEY,
        "http://digir.sourceforge.net/schema/conceptual/darwin/full/2003/1.0/darwin2full.xsd")
      .build();

    ImmutableMap<String, String> manisDigir = ImmutableMap.<String, String>builder()
      .put(SCHEMA_LOCATION_KEY, "http://bnhm.berkeley.edu/manis/DwC/darwin2jrw030315.xsd")
      .put(RECORD_SCHEMA_LOCATION_KEY, "http://bnhm.berkeley.edu/manis/DwC/darwin2resultfull.xsd")
      .build();

    SCHEMA_INFOS = ImmutableMap.<Boolean, ImmutableMap<String, String>>builder()
      .put(true, manisDigir)
      .put(false, normalDigir)
      .build();
  }

  private final ImmutableMap<String, Object> defaultContext;

  /**
   * Initializes this request handler.
   */
  public DigirScientificNameRangeRequestHandler(DigirCrawlConfiguration configuration) {
    super(configuration.getUrl(), TEMPLATE_LOCATION, REQUEST_PARAM_KEY);

    defaultContext = ImmutableMap.<String, Object>builder()
      .put("resource", configuration.getResourceCode())
      .put("destination", configuration.getUrl().toString())
      .put("maxResults", Integer.toString(MAX_RESULTS))
      .put(SCHEMA_LOCATION_KEY, SCHEMA_INFOS.get(configuration.isManis()).get(SCHEMA_LOCATION_KEY))
      .put(RECORD_SCHEMA_LOCATION_KEY, SCHEMA_INFOS.get(configuration.isManis()).get(RECORD_SCHEMA_LOCATION_KEY))
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
