package org.gbif.crawler.dwca;

import org.gbif.cli.IgnoreProperty;
import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.RegistryConfiguration;

import java.io.File;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;

public class DwcaConfiguration {

  public static final String DWCA_SUFFIX = ".dwca";
  public static final String METADATA_FILE = "metadata.xml";

  @ParametersDelegate
  @Valid
  @NotNull
  @IgnoreProperty
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public RegistryConfiguration registry = new RegistryConfiguration();

  @Parameter(names = "--pool-size")
  @Min(1)
  public int poolSize = 10;

  @Parameter(names = "--archive-repository")
  @NotNull
  public File archiveRepository;

  @Parameter(names = "--unpacked-repository")
  @NotNull
  public File unpackedRepository;
  @Parameter(names = "--archive-extract-directory")
  @NotNull
  public File archiveExtractDirectory;

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("registry", registry)
      .add("messaging", messaging)
      .add("poolSize", poolSize)
      .add("archiveRepository", archiveRepository)
      .add("unpackedRepository", unpackedRepository)
      .toString();
  }

}
