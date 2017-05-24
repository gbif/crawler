package org.gbif.crawler.ws;

import org.gbif.ws.server.interceptor.NullToNotFound;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expose the dataset overcrawl report.
 *
 * Note: overcrawledReport is expected to be updated on a regular basic on the file system.
 */
@Produces(MediaType.APPLICATION_JSON)
@Path("dataset/overcrawled")
public class OvercrawledResource {

  private static final Logger LOG = LoggerFactory.getLogger(OvercrawledResource.class);

  private final File overcrawledReport;

  @Inject
  public OvercrawledResource(@Named("overcrawledReportFilePath") String overcrawledReportFilePath) {
    this.overcrawledReport = new File(overcrawledReportFilePath);
  }

  @GET
  @NullToNotFound
  public String getOvercrawledReport() {
    /**
     * At the moment we blindly expose the report as String since there is no need to parse it.
     * It might be interesting to support query parameters and return only the relevant part of the report
     * but it should be small enough to be done client side.
     */
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
