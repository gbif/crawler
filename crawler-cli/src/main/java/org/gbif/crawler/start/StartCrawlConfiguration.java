package org.gbif.crawler.start;

import org.gbif.common.messaging.api.messages.StartCrawlMessage;
import org.gbif.common.messaging.config.MessagingConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class StartCrawlConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--dataset-uuid")
  @NotNull
  public String uuid;

  @Parameter(names = "--priority")
  @NotNull
  public StartCrawlMessage.Priority priority = StartCrawlMessage.Priority.NORMAL;

}
