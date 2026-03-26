package org.gbif.crawler.coldp;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.io.File;

import lombok.ToString;

import org.gbif.cli.PropertyName;
import org.gbif.crawler.common.RegistryConfiguration;
import org.gbif.crawler.common.crawlserver.CrawlServerConfiguration;

@ToString(callSuper = true)
public class ColDpConfiguration extends CrawlServerConfiguration {

  public static final String COLDP_SUFFIX = ".coldp";

  @ParametersDelegate @Valid @NotNull
  public RegistryConfiguration registry = new RegistryConfiguration();

  @Parameter(names = "--queue-name")
  public String queueName;

  @Parameter(names = "--archive-repository")
  @NotNull
  public File archiveRepository;

  @Parameter(names = "--unpacked-coldp-repository") // is this necessary?
  @NotNull
  public File unpackedColDpRepository;

  @Parameter(names = "--docker-image")
  @NotNull
  public String dockerImage;

  @Parameter(names = "--http-timeout", description = "Timeout for HTTP calls, milliseconds")
  @Min(1_000)
  @PropertyName("httpTimeout")
  public int httpTimeout = 10 * 60 * 1_000;

}
