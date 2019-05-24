package org.gbif.crawler.pipelines.xml;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.converters.XmlToAvroConverter;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.PipelineCallback.Steps;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.constants.PipelinesNodePaths.XML_TO_VERBATIM;
import static org.gbif.crawler.pipelines.HdfsUtils.buildOutputPath;
import static org.gbif.crawler.pipelines.PipelineCallback.Steps.ALL;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Call back which is called when the {@link PipelinesXmlMessage} is received.
 * <p>
 * The main method is {@link XmlToAvroCallback#handleMessage}
 */
public class XmlToAvroCallback extends AbstractMessageCallback<PipelinesXmlMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(XmlToAvroCallback.class);

  private static final String TAR_EXT = ".tar.xz";

  private final XmlToAvroConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;

  public XmlToAvroCallback(XmlToAvroConfiguration config, MessagePublisher publisher, CuratorFramework curator) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.publisher = publisher;
  }

  /**
   * Handles a MQ {@link PipelinesXmlMessage} message
   */
  @Override
  public void handleMessage(PipelinesXmlMessage message) {

    if (message.getPipelineSteps().isEmpty()) {
      message.setPipelineSteps(Collections.singleton(ALL.name()));
    }

    // Common variables
    UUID datasetId = message.getDatasetUuid();
    int attempt = message.getAttempt();
    Set<String> steps = message.getPipelineSteps();
    Runnable runnable = createRunnable(message);
    EndpointType endpointType = message.getEndpointType();
    ValidationResult validationResult = new ValidationResult(true, true, null);

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
    PipelineCallback.create()
        .incomingMessage(message)
        .outgoingMessage(new PipelinesVerbatimMessage(datasetId, attempt, config.interpretTypes, steps, null,
            endpointType, validationResult))
        .curator(curator)
        .zkRootElementPath(XML_TO_VERBATIM)
        .pipelinesStepName(Steps.XML_TO_VERBATIM.name())
        .publisher(publisher)
        .runnable(runnable)
        .build()
        .handleMessage();
  }

  /**
   * Main message processing logic, converts an ABCD archive to an avro file.
   */
  private Runnable createRunnable(PipelinesXmlMessage message) {
    return () -> {
      UUID datasetId = message.getDatasetUuid();
      String attempt = String.valueOf(message.getAttempt());

      // Calculates and checks existence of DwC Archive
      Path inputPath = buildInputPath(datasetId, attempt);

      // Calculates export path of avro as extended record
      org.apache.hadoop.fs.Path outputPath =
          buildOutputPath(config.repositoryPath, datasetId.toString(), attempt, config.fileName);

      // Calculates metadata path, the yaml file with total number of converted records
      org.apache.hadoop.fs.Path metaPath =
          buildOutputPath(config.repositoryPath, datasetId.toString(), attempt, config.metaFileName);

      // Run main conversion process
      XmlToAvroConverter.create()
          .xmlReaderParallelism(config.xmlReaderParallelism)
          .codecFactory(config.avroConfig.getCodec())
          .syncInterval(config.avroConfig.syncInterval)
          .hdfsSiteConfig(config.hdfsSiteConfig)
          .inputPath(inputPath)
          .outputPath(outputPath)
          .metaPath(metaPath)
          .idHashPrefix(datasetId.toString())
          .convert();
    };
  }

  /**
   * Input path result example, directory - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2,
   * if directory is absent, tries check a tar archive  - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2.tar.xz
   */
  private Path buildInputPath(UUID dataSetUuid, String attempt) {

    Path directoryPath = config.archiveRepositorySubdir.stream()
        .map(subdir -> Paths.get(config.archiveRepository, subdir, dataSetUuid.toString()).toFile())
        .filter(File::exists)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Can't find directory for dataset - " + dataSetUuid))
        .toPath();

    // Check dir, as an example - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2
    Path sibling = directoryPath.resolve(String.valueOf(attempt));
    if (sibling.toFile().exists()) {
      return sibling;
    }

    // Check dir, as an example - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2.tar.xz
    sibling = directoryPath.resolve(attempt + TAR_EXT);
    if (sibling.toFile().exists()) {
      return sibling;
    }

    // TODO: Do we need this? Try to find last attempt
    try (Stream<Path> walk = Files.list(directoryPath)) {
      String parsedAttempt = walk.map(Path::toFile)
          .map(File::getName)
          .map(name -> name.replace(TAR_EXT, ""))
          .filter(f -> f.matches("[0-9]+"))
          .map(Integer::valueOf)
          .max(Integer::compareTo)
          .map(String::valueOf)
          .orElse("0");

      // Check dir, as an example - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2
      sibling = directoryPath.resolve(parsedAttempt);
      if (sibling.toFile().exists()) {
        return sibling;
      }

      // Check dir, as an example - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2.tar.xz
      sibling = directoryPath.resolve(parsedAttempt + TAR_EXT);
      if (sibling.toFile().exists()) {
        return sibling;
      }
    } catch (IOException ex) {
      LOG.error(ex.getMessage(), ex);
    }

    // Return general
    return directoryPath;

  }
}
