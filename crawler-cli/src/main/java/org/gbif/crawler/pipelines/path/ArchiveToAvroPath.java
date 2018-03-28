package org.gbif.crawler.pipelines.path;

import org.gbif.crawler.pipelines.ConverterConfiguration;

import java.util.Arrays;
import java.util.StringJoiner;
import java.util.UUID;

import org.apache.hadoop.fs.Path;

/**
 * Contains derived paths for input and output
 */
public abstract class ArchiveToAvroPath {

  private Path outputPath;
  private java.nio.file.Path inputPath;

  /**
   * Derives source files path and target avro files path from configuration and received message.
   *
   * @return derived input, output paths
   */
  public ArchiveToAvroPath from(ConverterConfiguration configuration, UUID dataSetUuid, int attempt) {
    //calculates and checks existence of DwC Archive
    inputPath = buildInputPath(configuration.archiveRepository, dataSetUuid, attempt);
    //calculates export path of avro as extended record
    outputPath = buildOutputPath(configuration.extendedRecordRepository, dataSetUuid.toString(), String.valueOf(attempt), "verbatim.avro");

    return this;
  }

  /**
   * Target path where the avro files in {@link org.gbif.pipelines.io.avro.ExtendedRecord} format is written.
   *
   * @return extendedRepository export path
   */
  public Path getOutputPath() {
    return outputPath;
  }

  /**
   * Source path, required for reading the dataset.
   *
   * @return dataset path
   */
  public java.nio.file.Path getInputPath() {
    return inputPath;
  }

  /**
   * Store an Avro file on HDFS in /data/ingest/<datasetUUID>/<attemptID>/verbatim.avro
   */
  private Path buildOutputPath(String... values) {
    StringJoiner joiner = new StringJoiner(Path.SEPARATOR);
    Arrays.stream(values).forEach(joiner::add);
    return new Path(joiner.toString());
  }

  abstract java.nio.file.Path buildInputPath(String archiveRepository, UUID dataSetUuid, int attempt);

}
