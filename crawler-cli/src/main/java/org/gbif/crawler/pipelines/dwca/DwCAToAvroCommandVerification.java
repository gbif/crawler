package org.gbif.crawler.pipelines.dwca;

import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Verifies the configuration parameters like existence of input and output path, and enables with calculated configuration parameters helping DwCAToAvro Conversion
 */
public class DwCAToAvroCommandVerification {

  private final DwCAToAvroConfiguration configuration;
  private DwcaValidationFinishedMessage receivedMessage;

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

    //calculates and checks existence of DwC Archive
    String inputBasePath = configuration.archiveRepository.endsWith(File.separator)
      ? configuration.archiveRepository
      : configuration.archiveRepository + File.separator;
    String absoluteDwCAExportPath = inputBasePath + receivedMessage.getDatasetUuid().toString();
    String absoluteDwCAPath = absoluteDwCAExportPath + ".zip";
    Preconditions.checkState(new File(absoluteDwCAPath).exists(),
                             "Illegal Argument " + absoluteDwCAPath + " Input DwC Archive not available");
    //get underlying filesystem
    FileSystem fs = getFileSystem();

    //checks existence of provided path for exporting final avro file
    String baseURL = configuration.extendedRecordRepository.endsWith(File.separator)
      ? configuration.extendedRecordRepository
      : configuration.extendedRecordRepository + Path.SEPARATOR;
    try {
      Preconditions.checkState(fs.exists(new Path(baseURL)), "export avro url provided donot exists or is invalid");
    } catch (IOException e) {
      throw new IllegalArgumentException("file system or path has error, " + e.getMessage());

    }

    Path absoluteAvroExportPath = new Path(baseURL
                                           + receivedMessage.getDatasetUuid()
                                           + Path.SEPARATOR
                                           + receivedMessage.getAttempt()
                                           + "_verbatim.avro");
    return new DwCA2AvroConfigurationParameter(fs, absoluteAvroExportPath, absoluteDwCAPath, absoluteDwCAExportPath);

  }

  /**
   * Helper method to get file system based on provided configuration
   */
  private FileSystem getFileSystem() {
    try {
      return FileSystem.get(URI.create(configuration.extendedRecordRepository), new Configuration());
    } catch (IOException ex) {
      throw new IllegalStateException("Cannot get a valid filesystem from provided uri "
                                      + configuration.extendedRecordRepository, ex);
    }
  }

  /**
   * Calculated Configuration parameters
   */
  static class DwCA2AvroConfigurationParameter {

    private final FileSystem fs;
    private final Path absoluteDatasetExportPath;
    private final String absoluteDwCAPath;
    private final String absoluteDwCAExportPath;

    public DwCA2AvroConfigurationParameter(FileSystem fs, Path absoluteDatasetExportPath,
                                           String absoluteDwCAPath, String absoluteDwCAExportPath) {
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
