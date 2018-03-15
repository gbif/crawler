package org.gbif.crawler.pipelines.dwca;

import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.jdt.internal.core.Assert;

/**
 * Verifies the configuration parameters like existence of input and output path, and enables with calculated configuration parameters helping DwCAToAvro Conversion
 */
public class DwCAToAvroCommandVerification {

  private DwCAToAvroConfiguration configuration;
  private DwcaValidationFinishedMessage receivedMessage;
  private DwCA2AvroConfigurationParameter resourceConfigurationParameter;

  public static DwCAToAvroCommandVerification of(DwCAToAvroConfiguration configuration) {
    return new DwCAToAvroCommandVerification(configuration);
  }

  private DwCAToAvroCommandVerification(DwCAToAvroConfiguration configuration) {
    this.configuration = configuration;
  }

  public DwCAToAvroCommandVerification with(DwcaValidationFinishedMessage receivedMessage) {
    this.receivedMessage = receivedMessage;
    return this;
  }

  /**
   * verifies parameters and return calculated configuration parameters
   *
   * @return Calculated Configuration Parameters
   */
  public DwCA2AvroConfigurationParameter verifyParametersAndGetResourceConfigurations() {
    /*
      verifies the input configuration and messages are not null
     */
    Objects.requireNonNull(configuration, "Configuration cannot be null");
    Objects.requireNonNull(receivedMessage, "Received message cannot be null");
    /*
      checks existence of archiveRepository provided in the yaml config
     */
    String inputBasePath = configuration.archiveRepository.endsWith(File.separator)
      ? configuration.archiveRepository
      : configuration.archiveRepository.concat(File.separator);
    Assert.isLegal(Files.exists(new File(inputBasePath).toPath()),
                   "Illegal Argument archiveRepository is not a valid path");
    /*
      calculates and checks existence of DwC Archive
    */
    String absoluteDwCAExportPath = inputBasePath.concat(this.receivedMessage.getDatasetUuid().toString());
    String absoluteDwCAPath = absoluteDwCAExportPath.concat(".zip");
    Assert.isLegal(Files.exists(new File(absoluteDwCAPath).toPath()),
                   "Illegal Argument " + absoluteDwCAPath + " Input DwC Archive not available");

    Configuration config = new Configuration();
    config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    URI fsURI = URI.create(configuration.exportAvroBaseURL);
    FileSystem fs;
    try {
      fs = FileSystem.get(fsURI, config);
    } catch (IOException ex) {
      throw new IllegalArgumentException("Cannot get a valid filesystem from provided uri "
                                         + configuration.exportAvroBaseURL, ex);
    }
    /*
    checks existence of provided path for exporting final avro file
     */
    String baseURL = configuration.exportAvroBaseURL.endsWith(File.separator)
      ? configuration.exportAvroBaseURL
      : configuration.exportAvroBaseURL + File.separator;

    try {
      Assert.isLegal(fs.exists(new Path(baseURL)), "export avro url provided donot exists or is invalid");
    } catch (IOException e) {
      throw new IllegalArgumentException("file system or path has error, " + e.getMessage());

    }

    Path absoluteAvroExportPath = new Path(baseURL
                                           + "data"
                                           + File.separator
                                           + "ingest"
                                           + File.separator
                                           + receivedMessage.getDatasetUuid()
                                           + File.separator
                                           + receivedMessage.getAttempt()
                                           + "_verbatim.avro");
    return new DwCA2AvroConfigurationParameter(fs, absoluteAvroExportPath, absoluteDwCAPath, absoluteDwCAExportPath);

  }

  /**
   * Calculated Configuration parameters
   */
  class DwCA2AvroConfigurationParameter {

    private final FileSystem fs;
    private final Path absoluteDatasetExportPath;
    private final String absoluteDwCAPath;
    private final String absoluteDwCAExportPath;

    public DwCA2AvroConfigurationParameter(
      FileSystem fs, Path absoluteDatasetExportPath, String absoluteDwCAPath, String absoluteDwCAExportPath
    ) {
      this.fs = fs;
      this.absoluteDatasetExportPath = absoluteDatasetExportPath;
      this.absoluteDwCAPath = absoluteDwCAPath;
      this.absoluteDwCAExportPath = absoluteDwCAExportPath;
    }

    public FileSystem getFs() {
      return fs;
    }

    /**
     * absolute path for exporting avro file for provided dataset
     *
     * @return targetPath
     */
    public Path getAbsoluteDatasetExportPath() {
      return absoluteDatasetExportPath;
    }

    /**
     * absolute DwCA archive path for the provided dataset
     *
     * @return inputArchivePath
     */
    public String getAbsoluteDwCAPath() {
      return absoluteDwCAPath;
    }

    /**
     * target directory for extracting DwCA dataset archive
     */
    public String getAbsoluteDwCAExportPath() {
      return absoluteDwCAExportPath;
    }
  }

}
