package org.gbif.crawler.emlpusher;

import java.io.File;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;

public class PushEmlConfiguration {

  @Parameter(names = "--registry-ws-url")
  @NotNull
  public String registryWsUrl = "http://api.gbif.org/v0.9/";

  @Parameter(names = "--registry-user")
  @NotNull
  public String registryUser = "eml-pusher@gbif.org";

  @Parameter(names = "--registry-password")
  @NotNull
  public String registryPassword;

  @Parameter(names = "--archive-repository")
  @NotNull
  public File archiveRepository;
}
