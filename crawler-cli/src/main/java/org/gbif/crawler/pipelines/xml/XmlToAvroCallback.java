package org.gbif.crawler.pipelines.xml;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.converters.XmlToAvroConverter;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.PipelineCallback.Steps;

import org.apache.curator.framework.CuratorFramework;

import com.google.common.base.Preconditions;

import static org.gbif.crawler.constants.PipelinesNodePaths.XML_TO_VERBATIM;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Call back which is called when the {@link PipelinesXmlMessage} is received.
 */
public class XmlToAvroCallback extends AbstractMessageCallback<PipelinesXmlMessage> {

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

    // Common variables
    UUID datasetId = message.getDatasetUuid();
    int attempt = message.getAttempt();
    Set<String> steps = message.getPipelineSteps();
    Runnable runnable = createRunnable(message);

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
    PipelineCallback.create()
        .incomingMessage(message)
        .outgoingMessage(new PipelinesVerbatimMessage(datasetId, attempt, config.interpretTypes, steps))
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

      //calculates and checks existence of DwC Archive
      Path inputPath = buildInputPath(config.archiveRepository, datasetId, attempt);
      //calculates export path of avro as extended record
      org.apache.hadoop.fs.Path outputPath =
          buildOutputPath(config.repositoryPath, datasetId.toString(), attempt, config.fileName);

      XmlToAvroConverter.create()
          .xmlReaderParallelism(config.xmlReaderParallelism)
          .codecFactory(config.avroConfig.getCodec())
          .syncInterval(config.avroConfig.syncInterval)
          .hdfsSiteConfig(config.hdfsSiteConfig)
          .convert(inputPath, outputPath);
    };
  }

  /**
   * Input path result example, directory - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2,
   * if directory is absent, tries check a tar archive  - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2.tar.xz
   */
  private Path buildInputPath(String archiveRepository, UUID dataSetUuid, String attempt) {
    Path directoryPath = Paths.get(archiveRepository, dataSetUuid.toString(), String.valueOf(attempt));
    if (!directoryPath.toFile().exists()) {
      String filePath = directoryPath.toString() + ".tar.xz";
      Path archivePath = Paths.get(filePath);
      Preconditions.checkState(archivePath.toFile().exists(), "Directory - %s or archive %s does not exist!",
          directoryPath, archivePath);
      return archivePath;
    }
    return directoryPath;
  }

  /**
   * Store an Avro file on HDFS in /data/ingest/<datasetUUID>/<attemptID>/verbatim.avro
   */
  private org.apache.hadoop.fs.Path buildOutputPath(String... values) {
    StringJoiner joiner = new StringJoiner(org.apache.hadoop.fs.Path.SEPARATOR);
    Arrays.stream(values).forEach(joiner::add);
    return new org.apache.hadoop.fs.Path(joiner.toString());
  }
}
