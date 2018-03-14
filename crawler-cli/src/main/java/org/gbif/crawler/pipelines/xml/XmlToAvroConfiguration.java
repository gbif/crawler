package org.gbif.crawler.pipelines.xml;

import org.gbif.common.messaging.config.MessagingConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;

/**
 * Configuration to be used in the {@link XmlToAvroCommand}.
 */
public class XmlToAvroConfiguration {

  @ParametersDelegate
  @NotNull
  @Valid
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Parameter(names = "--thread-count", description = "How many threads should this service use to service requests in parallel")
  @Min(1)
  public int threadCount = 1;

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("messaging", messaging)
      .add("queueName", queueName)
      .add("threadCount", threadCount)
      .toString();
  }

}
