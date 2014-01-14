package org.gbif.crawler.dwca.downloader;

import org.gbif.crawler.common.crawlserver.CrawlServerConfiguration;

import java.io.File;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.google.common.base.Objects;

public class DownloaderConfiguration extends CrawlServerConfiguration {

  @Parameter(names = "--archive-repository")
  @NotNull
  public File archiveRepository;

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("super", super.toString())
      .add("archiveRepository", archiveRepository)
      .toString();
  }

}
