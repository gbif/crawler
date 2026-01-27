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
package org.gbif.crawler.xml.crawlserver.listener;

import org.gbif.crawler.AbstractCrawlListener;
import org.gbif.crawler.CrawlConfiguration;
import org.gbif.crawler.strategy.ScientificNameRangeCrawlContext;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.google.common.primitives.Bytes;

/**
 * This Crawl Listener will persist every single response we receive to disk. It will do so in sub
 * folders of the given base path in a format like this:
 * /<uuid>/<attempt>/<context>/<uuid>_<attempt>_<context>_<retry>.response
 */
public class ResultPersistingListener
    extends AbstractCrawlListener<ScientificNameRangeCrawlContext, String, List<Byte>> {

  private static final Logger LOG = LoggerFactory.getLogger(ResultPersistingListener.class);

  private final File basePath;
  private final CrawlConfiguration configuration;

  public ResultPersistingListener(File basePath, CrawlConfiguration configuration) {
    this.basePath = basePath;
    this.configuration = configuration;
  }

  @Override
  public void response(
      List<Byte> response,
      int retry,
      long duration,
      Optional<Integer> recordCount,
      Optional<Boolean> endOfRecords) {
    String contextString =
        getCurrentContext().getLowerBound().orElse("null")
            + "-"
            + getCurrentContext().getUpperBound().orElse("null");
    if (response == null || response.isEmpty()) {
      LOG.info(
          "Received empty response for [{}] [{}] in retry [{}]",
          configuration.getDatasetKey(),
          contextString,
          retry);
    }
    // This is guaranteed to be efficient as documented on the AbstractResponseHandler
    byte[] bytes = Bytes.toArray(response);
    try {
      // Creates a format like this:
      // /<uuid>/<attempt>/<context>/<uuid>_<attempt>_<context>_<retry>.response
      StringBuilder path = new StringBuilder();
      path.append(configuration.getDatasetKey())
          .append(File.separator)
          .append(configuration.getAttempt())
          .append(File.separator)
          .append(contextString)
          .append(File.separator)
          .append(configuration.getDatasetKey())
          .append("_")
          .append(configuration.getAttempt())
          .append("_")
          .append(contextString)
          .append("_")
          .append(getCurrentContext().getOffset())
          .append("_")
          .append(retry)
          .append(".response");

      File file = new File(basePath, path.toString());
      Files.createParentDirs(file);
      Files.write(bytes, file);
    } catch (IOException e) {
      LOG.warn("Could not write response due to exception. [{}]", configuration.getDatasetKey(), e);
    }
  }
}
