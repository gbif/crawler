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
package org.gbif.crawler.protocol;

import org.gbif.crawler.RequestHandler;
import org.gbif.crawler.strategy.ScientificNameRangeCrawlContext;
import org.gbif.crawler.util.TemplateUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.client.utils.URIBuilder;

import com.google.common.io.Resources;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A abstract class that is extended by the BioCASe and DiGIR request builders because their request
 * URLs are very similar to build.
 *
 * <p>All they need to do is provide the correct constructor parameters and implement the {@link
 * #getDefaultContext()} method.
 */
public abstract class AbstractScientificNameRangeRequestHandler
    implements RequestHandler<ScientificNameRangeCrawlContext, String> {

  private final String rangeTemplateLocation;
  private final String nullTemplateLocation;
  private final String requestParamKey;
  private final URI targetUrl;

  /**
   * All constructor parameters are mandatory and may not be null.
   *
   * @param targetUrl of the dataset
   * @param rangeTemplateLocation the location on the classpath that contains the template to use
   *     for range requests
   * @param nullTemplateLocation the location on the classpath that contains the template to use for
   *     null requests
   * @param requestParamKey the query parameter name that contains the serialized request
   */
  protected AbstractScientificNameRangeRequestHandler(
      URI targetUrl,
      String rangeTemplateLocation,
      String nullTemplateLocation,
      String requestParamKey) {
    this.targetUrl = checkNotNull(targetUrl);
    this.rangeTemplateLocation = checkNotNull(rangeTemplateLocation);
    this.nullTemplateLocation = checkNotNull(nullTemplateLocation);
    this.requestParamKey = checkNotNull(requestParamKey);

    // Throws an IllegalArgumentException if it doesn't exist so serves as validation
    Resources.getResource(rangeTemplateLocation);
    Resources.getResource(nullTemplateLocation);
  }

  @Override
  public String buildRequestUrl(ScientificNameRangeCrawlContext context) {
    checkNotNull(context);

    try {
      return new URIBuilder(targetUrl)
          .addParameter(requestParamKey, buildRequest(context))
          .build()
          .toString();
    } catch (URISyntaxException e) {
      // It's coming from a valid URL so it shouldn't ever throw this, so we swallow this
      return null;
    }
  }

  /**
   * @param context The request context
   * @return the request, not the full URL
   */
  private String buildRequest(ScientificNameRangeCrawlContext context) {
    Map<String, Object> templateContext = new HashMap<String, Object>(getDefaultContext());
    String templateLocation = nullTemplateLocation;

    templateContext.put("startAt", String.valueOf(context.getOffset()));

    if (context.getLowerBound().isPresent()) {
      templateContext.put("lower", context.getLowerBound().get());
      templateLocation = rangeTemplateLocation;
    }
    if (context.getUpperBound().isPresent()) {
      templateContext.put("upper", context.getUpperBound().get());
      templateLocation = rangeTemplateLocation;
    }

    try {
      return TemplateUtils.getAndProcess(templateLocation, templateContext);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to generate request", e);
    }
  }

  protected abstract Map<String, Object> getDefaultContext();
}
