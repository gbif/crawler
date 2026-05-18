package org.gbif.crawler.dwcdp.metasync;

import org.gbif.cli.IgnoreProperty;
import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.RegistryConfiguration;
import org.gbif.crawler.common.ZooKeeperConfiguration;

import java.io.File;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.ToString;

@ToString
public class DwcDpMetasyncConfiguration {

  @ParametersDelegate @Valid @NotNull @IgnoreProperty
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate @Valid @NotNull
  public ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

  @ParametersDelegate @Valid @NotNull
  public RegistryConfiguration registry = new RegistryConfiguration();

  @Parameter(names = "--pool-size")
  @Min(1)
  public int poolSize = 10;

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Parameter(names = "--archive-repository")
  @NotNull
  public File archiveRepository;
}
