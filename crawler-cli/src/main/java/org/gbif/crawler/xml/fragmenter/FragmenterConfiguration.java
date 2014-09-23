package org.gbif.crawler.xml.fragmenter;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.ZooKeeperConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;

public class FragmenterConfiguration {

  @ParametersDelegate
  @NotNull
  @Valid
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

  @Parameter(names = "--pool-size")
  @Min(1)
  public int poolSize = 10;

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Parameter(names = "--crawler-ws-url")
  @NotNull
  public String crawlerWsUrl;

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("messaging", messaging).add("zooKeeper", zooKeeper)
      .add("poolSize", poolSize).add("queueName", queueName).add("crawlerWsUrl", crawlerWsUrl).toString();
  }

}
