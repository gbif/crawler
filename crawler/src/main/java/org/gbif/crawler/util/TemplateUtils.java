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
package org.gbif.crawler.util;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

/** Utilities for working with Freemarker templates. */
public class TemplateUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TemplateUtils.class);

  private static final Configuration CONFIGURATION = new Configuration();

  static {
    CONFIGURATION.setClassForTemplateLoading(TemplateUtils.class, "/");
  }

  /**
   * Processes the template at the given location with the parameters provided to a String.
   *
   * @param location from the root of the classpath - e.g. org/test/mytemplate.ftl
   * @param parameters to substitute in the template
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
   * @return the template
   * @throws IOException On error - normally the location cannot be found
   */
  public static Template getTemplate(String location) throws IOException {
    return CONFIGURATION.getTemplate(location);
  }

  /** Processes the template and returns it as a string. */
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
