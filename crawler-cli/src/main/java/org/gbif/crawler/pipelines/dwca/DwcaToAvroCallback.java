package org.gbif.crawler.pipelines.dwca;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;

import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.converters.DwcaToAvroConverter;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.PipelineCallback.Steps;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import static org.gbif.api.vocabulary.DatasetType.OCCURRENCE;
import static org.gbif.api.vocabulary.DatasetType.SAMPLING_EVENT;
import static org.gbif.crawler.constants.PipelinesNodePaths.DWCA_TO_VERBATIM;
import static org.gbif.crawler.pipelines.HdfsUtils.buildOutputPath;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Callback which is called when the {@link PipelinesDwcaMessage} is received.
 * <p>
 * The main method is {@link DwcaToAvroCallback#handleMessage}
 */
public class DwcaToAvroCallback extends AbstractMessageCallback<PipelinesDwcaMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToAvroCallback.class);

  private final DwcaToAvroConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;

  DwcaToAvroCallback(DwcaToAvroConfiguration config, MessagePublisher publisher, CuratorFramework curator) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.publisher = publisher;
  }

  /**
   * Handles a MQ {@link PipelinesDwcaMessage} message
   */
  @Override
  public void handleMessage(PipelinesDwcaMessage message) {

    MDC.put("datasetId", message.getDatasetUuid().toString());
    MDC.put("attempt", String.valueOf(message.getAttempt()));
    LOG.info("Message handler began - {}", message);

    if (!isMessageCorrect(message)) {
      return;
    }

    if (message.getPipelineSteps().isEmpty()) {
      message.setPipelineSteps(Sets.newHashSet(
          Steps.DWCA_TO_VERBATIM.name(),
          Steps.VERBATIM_TO_INTERPRETED.name(),
          Steps.INTERPRETED_TO_INDEX.name()
      ));
    }

    // Common variables
    UUID datasetId = message.getDatasetUuid();
    int attempt = message.getAttempt();
    Set<String> steps = message.getPipelineSteps();
    Runnable runnable = createRunnable(message);
    EndpointType endpointType = message.getEndpointType();
    OccurrenceValidationReport occReport = message.getValidationReport().getOccurrenceReport();
    Long numberOfRecords = occReport == null ? null : (long) occReport.getCheckedRecords();
    ValidationResult validationResult =
        new ValidationResult(tripletsValid(occReport), occurrenceIdsValid(occReport), null, numberOfRecords);

    // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
    PipelineCallback.create()
        .incomingMessage(message)
        .outgoingMessage(
            new PipelinesVerbatimMessage(datasetId, attempt, config.interpretTypes, steps, null, endpointType, null,
                validationResult)
        )
        .curator(curator)
        .zkRootElementPath(DWCA_TO_VERBATIM)
        .pipelinesStepName(Steps.DWCA_TO_VERBATIM.name())
        .publisher(publisher)
        .runnable(runnable)
        .build()
        .handleMessage();

    LOG.info("Message handler ended - {}", message);
  }

  /**
   * Only correct messages can be handled, by now is only OCCURRENCE type messages
   */
  private boolean isMessageCorrect(PipelinesDwcaMessage message) {
    return OCCURRENCE == message.getDatasetType() || SAMPLING_EVENT == message.getDatasetType();
  }

  /**
   * Main message processing logic, converts a DwCA archive to an avro file.
   */
  private Runnable createRunnable(PipelinesDwcaMessage message) {
    return () -> {

      UUID datasetId = message.getDatasetUuid();
      String attempt = String.valueOf(message.getAttempt());

      // Calculates and checks existence of DwC Archive
      Path inputPath = buildInputPath(config.archiveRepository, datasetId);

      // Calculates export path of avro as extended record
      org.apache.hadoop.fs.Path outputPath =
          buildOutputPath(config.repositoryPath, datasetId.toString(), attempt, config.fileName);

      // Calculates metadata path, the yaml file with total number of converted records
      org.apache.hadoop.fs.Path metaPath =
          buildOutputPath(config.repositoryPath, datasetId.toString(), attempt, config.metaFileName);

      // Run main conversion process
      DwcaToAvroConverter.create()
          .codecFactory(config.avroConfig.getCodec())
          .syncInterval(config.avroConfig.syncInterval)
          .hdfsSiteConfig(config.hdfsSiteConfig)
          .inputPath(inputPath)
          .outputPath(outputPath)
          .metaPath(metaPath)
          .convert();
    };
  }

  /**
   * Input path example - /mnt/auto/crawler/dwca/9bed66b3-4caa-42bb-9c93-71d7ba109dad
   */
  private Path buildInputPath(String archiveRepository, UUID dataSetUuid) {
    Path directoryPath = Paths.get(archiveRepository, dataSetUuid.toString());
    Preconditions.checkState(directoryPath.toFile().exists(), "Directory - %s does not exist!", directoryPath);

    return directoryPath;
  }

  /**
   * For XML datasets triplets are always valid. For DwC-A datasets triplets are valid if there are more than 0 unique
   * triplets in the dataset, and exactly 0 triplets referenced by more than one record.
   */
  private static boolean tripletsValid(OccurrenceValidationReport report) {
    if (report == null) {
      return true;
    }
    return report.getUniqueTriplets() > 0
        && report.getCheckedRecords() - report.getRecordsWithInvalidTriplets() == report.getUniqueTriplets();
  }

  /**
   * For XML datasets occurrenceIds are always accepted. For DwC-A datasets occurrenceIds are valid if each record has a
   * unique occurrenceId.
   */
  private static boolean occurrenceIdsValid(OccurrenceValidationReport report) {
    if (report == null) {
      return true;
    }
    return report.getCheckedRecords() > 0 && report.getUniqueOccurrenceIds() == report.getCheckedRecords();
  }

}
