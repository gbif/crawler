package org.gbif.crawler.ws;

import org.gbif.api.annotation.NullToNotFound;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Expose the dataset overcrawl report.
 *
 * Note: overcrawledReport is expected to be updated on a regular basic on the file system.
 */
@Primary
@RestController
@RequestMapping(value = "dataset/overcrawled", produces = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
public class OvercrawledResource {

  private static final Logger LOG = LoggerFactory.getLogger(OvercrawledResource.class);

  private final File overcrawledReport;

  @Autowired
  public OvercrawledResource(@Qualifier("overcrawledReportFilePath") String overcrawledReportFilePath) {
    this.overcrawledReport = new File(overcrawledReportFilePath);
  }

  @GetMapping
  @NullToNotFound("dataset/overcrawled")
  public String getOvercrawledReport() {
//  At the moment we blindly expose the report as String since there is no need to parse it.
//  It might be interesting to support query parameters and return only the relevant part of the report
//  but it should be small enough to be done client side.
    String jsonContent = null;

    if(overcrawledReport.exists()) {
      try {
        jsonContent = FileUtils.readFileToString(overcrawledReport, StandardCharsets.UTF_8);
      } catch (IOException e) {
        LOG.error("Can't load overcrawledReport", e);
      }
    }
    else{
      LOG.error("Can't load overcrawledReport from {}", overcrawledReport);
    }
    return jsonContent;
  }

}
