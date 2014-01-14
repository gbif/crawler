package org.gbif.crawler.util;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for working with Freemarker templates.
 */
public class TemplateUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TemplateUtils.class);

  private static final Configuration CONFIGURATION = new Configuration();

  static {
    CONFIGURATION.setClassForTemplateLoading(TemplateUtils.class, "/");
  }

  /**
   * Processes the template at the given location with the parameters provided to a String.
   *
   * @param location   from the root of the classpath - e.g. org/test/mytemplate.ftl
   * @param parameters to substitute in the template
   *
   * @return the String representation of the processed template
   */
  public static String getAndProcess(String location, Map<String, Object> parameters)
    throws IOException, TemplateException {
    Template template = getTemplate(location);
    return process(template, parameters);
  }

  /**
   * Gets the Freemarker template from the location provided.
   *
   * @param location to get the template from
   *
   * @return the template
   *
   * @throws IOException On error - normally the location cannot be found
   */
  public static Template getTemplate(String location) throws IOException {
    return CONFIGURATION.getTemplate(location);
  }

  /**
   * Processes the template and returns it as a string.
   */
  public static String process(Template template, Map<String, Object> parameters)
    throws IOException, TemplateException {
    StringWriter writer = new StringWriter();
    template.process(parameters, writer);
    return writer.toString();
  }

  private TemplateUtils() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

}
