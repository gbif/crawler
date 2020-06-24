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

import org.gbif.crawler.RequestHandler;
import org.gbif.crawler.strategy.ScientificNameRangeCrawlContext;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.http.client.utils.URIBuilder;

import com.google.common.collect.Maps;

/**
 * This is a request handler for TAPIR queries using a range of scientific names.
 *
 * <p>It supports DwC, ABCD 1.2, ABCD 2.05 and ABCD 2.06.
 *
 * <p>This class is not thread-safe.
 */
@NotThreadSafe
public class TapirScientificNameRangeRequestHandler
    implements RequestHandler<ScientificNameRangeCrawlContext, String> {

  // TAPIR supports multiple output formats, so we map a namespace to a search template with
  // appropriate output model
  private static final Map<String, String> TEMPLATE_MAPPING = new HashMap<String, String>();

  private static final int MAX_RESULTS = 1000;

  private final String contentNamespace;

  private final URI targetUrl;

  static {
    // DwC
    TEMPLATE_MAPPING.put(
        "http://rs.tdwg.org/dwc/dwcore/",
        "http://rs.gbif.org/templates/tapir/dwc/1.4/sci_name_range.xml");
    TEMPLATE_MAPPING.put(
        "http://rs.tdwg.org/dwc/geospatial/",
        "http://rs.gbif.org/templates/tapir/dwc/1.4/sci_name_range.xml");
    TEMPLATE_MAPPING.put(
        "http://rs.tdwg.org/dwc/curatorial/",
        "http://rs.gbif.org/templates/tapir/dwc/1.4/sci_name_range.xml");
    TEMPLATE_MAPPING.put(
        "http://rs.tdwg.org/dwc/terms/",
        "http://rs.tdwg.org/tapir/cs/dwc/terms/2009-09-23/template/dwc_sci_name_range.xml");
    TEMPLATE_MAPPING.put(
        "http://digir.net/schema/conceptual/darwin/2003/1.0",
        "http://rs.gbif.org/templates/tapir/dwc/1.0/sci_name_range.xml");
    TEMPLATE_MAPPING.put(
        "http://www.tdwg.org/schemas/abcd/1.2",
        "http://rs.gbif.org/templates/tapir/abcd/1.2/sci_name_range.xml");
    TEMPLATE_MAPPING.put(
        "http://www.tdwg.org/schemas/abcd/2.06",
        "http://rs.gbif.org/templates/tapir/abcd/206/sci_name_range.xml");
    // ABCD (Default to 2.0.6 whenever 2.0.5 is encountered)
    TEMPLATE_MAPPING.put(
        "http://www.tdwg.org/schemas/abcd/2.05",
        TEMPLATE_MAPPING.get("http://www.tdwg.org/schemas/abcd/2.06"));
  }

  public TapirScientificNameRangeRequestHandler(TapirCrawlConfiguration configuration) {
    contentNamespace = configuration.getContentNamespace();
    targetUrl = configuration.getUrl();
  }

  /** @return The atoms for the TAPIR request */
  private Map<String, String> buildTapirRequestParameters(ScientificNameRangeCrawlContext context) {
    Map<String, String> params = Maps.newLinkedHashMap();

    params.put("limit", String.valueOf(MAX_RESULTS));

    if (context.getUpperBound().isPresent()) {
      params.put("upper", context.getUpperBound().get());
    } else {
      params.put("upper", null);
    }

    if (context.getLowerBound().isPresent()) {
      params.put("lower", context.getLowerBound().get());
    } else {
      params.put("lower", null);
    }

    params.put("t", TEMPLATE_MAPPING.get(contentNamespace));

    params.put("op", "s");

    params.put("start", String.valueOf(context.getOffset()));

    return params;
  }

  @Override
  public String buildRequestUrl(ScientificNameRangeCrawlContext context) {
    try {
      URIBuilder uriBuilder = new URIBuilder(targetUrl);
      for (Map.Entry<String, String> paramEntry : buildTapirRequestParameters(context).entrySet()) {
        uriBuilder.addParameter(paramEntry.getKey(), paramEntry.getValue());
      }

      return uriBuilder.build().toString();
    } catch (URISyntaxException e) {
      // It's coming from a valid URL so it shouldn't ever throw this, so we swallow this
      return null;
    }
  }

  @Override
  public int getLimit() {
    return MAX_RESULTS;
  }
}
