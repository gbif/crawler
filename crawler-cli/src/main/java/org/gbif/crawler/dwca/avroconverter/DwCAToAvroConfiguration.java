package org.gbif.crawler.dwca.avroconverter;

import org.gbif.common.messaging.config.MessagingConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;

/**
 * Configuration required to convert downloaded DwCArchive to avro (ExtendedRecord)
 */
public class DwCAToAvroConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Parameter(names = "--parallelism")
  @NotNull
  @Min(1)
  public int parallelism;

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("queueName", queueName)
      .add("virtualHost", messaging.virtualHost)
      .add("userName", messaging.username)
      .add("password", messaging.password)
      .add("port", messaging.port)
      .add("connectionHost", messaging.host)
      .add("parallelism", parallelism)
      .toString();
  }
}
