package org.gbif.crawler.protocol;

import org.gbif.crawler.RequestHandler;
import org.gbif.crawler.strategy.ScientificNameRangeCrawlContext;
import org.gbif.crawler.util.TemplateUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.apache.http.client.utils.URIBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A abstract class that is extended by the BioCASe and DiGIR request builders because their request URLs are very
 * similar to build.
 * <p/>
 * All they need to do is provide the correct constructor parameters and implement the {@link #getDefaultContext()}
 * method.
 */
public abstract class AbstractScientificNameRangeRequestHandler
  implements RequestHandler<ScientificNameRangeCrawlContext, String> {

  private final String templateLocation;
  private final String requestParamKey;
  private final URI targetUrl;

  /**
   * All constructor parameters are mandatory and may not be null.
   *
   * @param targetUrl        of the dataset
   * @param templateLocation the location on the classpath that contains the template to use for this request
   * @param requestParamKey  the query parameter name that contains the serialized request
   */
  protected AbstractScientificNameRangeRequestHandler(URI targetUrl, String templateLocation, String requestParamKey) {
    this.targetUrl = checkNotNull(targetUrl);
    this.templateLocation = checkNotNull(templateLocation);
    this.requestParamKey = checkNotNull(requestParamKey);

    // Throws an IllegalArgumentException if it doesn't exist so serves as validation
    Resources.getResource(templateLocation);
  }

  @Override
  public String buildRequestUrl(ScientificNameRangeCrawlContext context) {
    checkNotNull(context);

    try {
      return new URIBuilder(targetUrl).addParameter(requestParamKey, buildRequest(context)).build().toString();
    } catch (URISyntaxException e) {
      // It's coming from a valid URL so it shouldn't ever throw this, so we swallow this
      return null;
    }
  }

  /**
   * @param context The request context
   *
   * @return the request, not the full URL
   */
  private String buildRequest(ScientificNameRangeCrawlContext context) {
    Map<String, Object> templateContext = new HashMap<String, Object>(getDefaultContext());

    templateContext.put("startAt", String.valueOf(context.getOffset()));

    if (context.getLowerBound().isPresent()) {
      templateContext.put("lower", context.getLowerBound().get());
    }
    if (context.getUpperBound().isPresent()) {
      templateContext.put("upper", context.getUpperBound().get());
    }

    try {
      return TemplateUtils.getAndProcess(templateLocation, templateContext);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to generate request", e);
    }
  }

  protected abstract ImmutableMap<String, Object> getDefaultContext();

}
