package org.gbif.crawler.pipelines;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.AvroWriteConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;
import org.apache.avro.file.CodecFactory;

/**
 * Configuration required to convert downloaded DwCArchive/ABCD and etc to avro (ExtendedRecord)
 */
public class ConverterConfiguration {

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
  public int poolSize;

  @Parameter(names = "--archive-repository")
  @NotNull
  public String archiveRepository;

  @Parameter(names = "--extended-record-repository")
  @NotNull
  public String extendedRecordRepository;

  @ParametersDelegate
  @Valid
  @NotNull
  public AvroWriteConfiguration avroConfig=new AvroWriteConfiguration();

  @Parameter(names = "--hdfs-site-config")
  public String hdfsSiteConfig;

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("queueName", queueName)
      .add("virtualHost", messaging.virtualHost)
      .add("userName", messaging.username)
      .add("password", messaging.password)
      .add("port", messaging.port)
      .add("connectionHost", messaging.host)
      .add("poolSize", poolSize)
      .add("archiveRepository", archiveRepository)
      .add("extendedRecordRepository", extendedRecordRepository)
      .add("syncInterval", avroConfig.syncInterval)
      .add("compressionCodec", avroConfig.compressionType)
      .add("codecFactory",avroConfig.getCodec())
      .add("hdfsSiteConfig", hdfsSiteConfig)
      .toString();
  }
}
