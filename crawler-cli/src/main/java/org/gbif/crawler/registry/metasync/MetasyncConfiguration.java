package org.gbif.crawler.registry.metasync;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.RegistryConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;

public class MetasyncConfiguration {

  @ParametersDelegate
  @NotNull
  @Valid
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate
  @NotNull
  @Valid
  public RegistryConfiguration registry = new RegistryConfiguration();

  @Parameter(names = "--thread-count",
    description = "How many threads should this service use to service requests in parallel")
  @Min(1)
  public int threadCount = 10;

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("messaging", messaging)
      .add("threadCount", threadCount)
      .add("queueName", queueName)
      .add("registry", registry)
      .toString();
  }

}
