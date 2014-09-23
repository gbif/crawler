package org.gbif.crawler.common.crawlserver;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.ZooKeeperConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;

@SuppressWarnings("PublicField")
public class CrawlServerConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

  @Parameter(names = "--pool-size",
    description = "Configures how many threads will be used to process items from the queue")
  @Min(1)
  public int poolSize = 1;

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("messaging", messaging).add("zooKeeper", zooKeeper)
      .add("poolSize", poolSize).toString();
  }

}
