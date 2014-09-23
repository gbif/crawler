package org.gbif.crawler.coordinator;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.RegistryConfiguration;
import org.gbif.crawler.common.ZooKeeperConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;

public class CoordinatorConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public RegistryConfiguration registry = new RegistryConfiguration();

  @Parameter(names = "--pool-size")
  @Min(1)
  public int poolSize = 5;

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName = "crawler_coordinator";

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("messaging", messaging).add("zooKeeper", zooKeeper)
      .add("registry", registry).add("poolSize", poolSize).add("queueName", queueName).toString();
  }
}
