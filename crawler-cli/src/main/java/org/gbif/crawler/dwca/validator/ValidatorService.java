package org.gbif.crawler.dwca.validator;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.crawler.ProcessState;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcaDownloadFinishedMessage;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.crawler.dwca.DwcaConfiguration;
import org.gbif.crawler.dwca.DwcaService;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.UnsupportedArchiveException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.UUID;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.gbif.crawler.common.ZookeeperUtils.createOrUpdate;
import static org.gbif.crawler.constants.CrawlerNodePaths.FINISHED_REASON;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_CHECKLIST;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_OCCURRENCE;
import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_SAMPLE;
import static org.gbif.dwc.terms.GbifTerm.datasetKey;

public class ValidatorService extends DwcaService {

  private static final Logger LOG = LoggerFactory.getLogger(ValidatorService.class);

  private final DwcaValidatorConfiguration configuration;


  public ValidatorService(DwcaValidatorConfiguration configuration) {
    super(configuration);
    this.configuration = configuration;
  }

  @Override
  protected void bindListeners() throws IOException {
    CuratorFramework curator = configuration.zooKeeper.getCuratorFramework();

    // listen to DwcaDownloadFinishedMessage messages
    listener.listen("dwca-validator", config.poolSize,
      new DwcaDownloadFinishedMessageCallback(datasetService, config.archiveRepository, config.unpackedRepository, publisher, curator));
  }

  private static class DwcaDownloadFinishedMessageCallback
    extends AbstractMessageCallback<DwcaDownloadFinishedMessage> {

    private final DatasetService datasetService;
    private final File archiveRepository;
    private final File unpackDirectory;
    private final MessagePublisher publisher;
    private final CuratorFramework curator;

    private final Counter messageCount = Metrics.newCounter(ValidatorService.class, "messageCount");
    private final Counter failedValidations = Metrics.newCounter(ValidatorService.class, "failedValidations");

    private DwcaDownloadFinishedMessageCallback(DatasetService datasetService, File archiveRepository,
      File unpackDirectory, MessagePublisher publisher, CuratorFramework curator) {
      this.datasetService = datasetService;
      this.archiveRepository = archiveRepository;
      this.unpackDirectory = unpackDirectory;
      this.publisher = publisher;
      this.curator = curator;
    }

    @Override
    public void handleMessage(DwcaDownloadFinishedMessage message) {
      messageCount.inc();

      final UUID datasetKey = message.getDatasetUuid();
      try (
        MDC.MDCCloseable ignored1 = MDC.putCloseable("datasetKey", datasetKey.toString());
        MDC.MDCCloseable ignored2 = MDC.putCloseable("attempt", String.valueOf(message.getAttempt()))
      ) {

        LOG.info("Now validating DwC-A for dataset [{}]", datasetKey);
        Dataset dataset = datasetService.get(datasetKey);
        if (dataset == null) {
          // exception, we don't know this dataset
          throw new IllegalArgumentException("The requested dataset " + datasetKey + " is not registered");
        }

        final Path dwcaFile = new File(archiveRepository, datasetKey + "/" + datasetKey + DwcaConfiguration.DWCA_SUFFIX).toPath();
        final Path destinationDir = new File(unpackDirectory, datasetKey.toString()).toPath();

        DwcaValidationReport validationReport = prepareAndRunValidation(dataset, dwcaFile, destinationDir);
        if (validationReport.isValid()) {
          updateProcessState(dataset, validationReport, ProcessState.RUNNING);

        } else {
          failedValidations.inc();
          createOrUpdate(curator, datasetKey, FINISHED_REASON, FinishReason.ABORT);
          updateProcessState(dataset, validationReport, ProcessState.FINISHED);
        }

        LOG.info("Finished validating DwC-A for dataset [{}], valid? is [{}]. Full report [{}]", datasetKey,
            validationReport.isValid(), validationReport);

        // send validation finished message
        try {
          publisher.send(
              new DwcaValidationFinishedMessage(datasetKey, dataset.getType(), message.getSource(), message.getAttempt(),
                  validationReport, message.getEndpointType(), message.getPlatform()), true);
        } catch (IOException e) {
          LOG.error("Failed to send validation finished message for dataset [{}]", datasetKey, e);
        }
      }
    }

    /**
     * Prepare the file(s) and run the validation on the resulting file(s).
     * @param dataset
     * @param downloadedFile
     * @param destinationFolder
     * @return
     */
    private DwcaValidationReport prepareAndRunValidation(Dataset dataset, Path downloadedFile, Path destinationFolder) {

      Objects.requireNonNull(dataset, "dataset shall be provided");
      DwcaValidationReport validationReport;

      try {
        if (DatasetType.METADATA == dataset.getType()) {
          Path destinationPath = destinationFolder.resolve(DwcaConfiguration.METADATA_FILE);
          FileUtils.forceMkdir(destinationFolder.toFile());
          // not an archive so copy the file alone
          // we copy the file (instead of using the .dwca directly) in case the dataset is transformed into a more concrete type eventually
          Files.copy(downloadedFile, destinationPath, StandardCopyOption.REPLACE_EXISTING);
          String metadata = new String(Files.readAllBytes(destinationPath), StandardCharsets.UTF_8);
          validationReport = DwcaValidator.validate(dataset, metadata);
        } else {
          Archive archive = DwcFiles.fromCompressed(downloadedFile, destinationFolder);
          validationReport = DwcaValidator.validate(dataset, archive);
        }
      } catch (UnsupportedArchiveException e) {
        LOG.warn("Invalid DwC archive for dataset [{}]", dataset.getKey(), e);
        validationReport = new DwcaValidationReport(dataset.getKey(), "Invalid Dwc archive");
      } catch (IOException e) {
        LOG.error("IOException when reading DwC archive for dataset [{}]", dataset.getKey(), e);
        validationReport = new DwcaValidationReport(dataset.getKey(), "IOException when reading DwC archive");
      } catch (RuntimeException e) {
        LOG.error("Unknown error when reading DwC archive for dataset [{}]", dataset.getKey(), e);
        validationReport = new DwcaValidationReport(dataset.getKey(), "Unexpected error when reading DwC archive: " + e.getMessage());
      }

      return validationReport;
    }

    /**
     * Set the process state.  For existing dataset types (that contains data) this sets the process state as given.
     * For METADATA, this method sets the state to EMPTY.
     */
    private void updateProcessState(Dataset dataset, DwcaValidationReport report, ProcessState state) {
      // Override state to EMPTY if there are no occurrences.
      ProcessState occurrenceState = (report.getOccurrenceReport() != null && report.getOccurrenceReport().getCheckedRecords() > 0)
        ? state : ProcessState.EMPTY;

      switch (dataset.getType()) {
        case OCCURRENCE:
          createOrUpdate(curator, report.getDatasetKey(), PROCESS_STATE_OCCURRENCE, occurrenceState);
          break;
        case CHECKLIST:
        case SAMPLING_EVENT:
          // we might have a mixed dataset with taxa or events and optionally also occurrences

          // update core status
          // if there is no report, we record empty, otherwise we record the given state
          ProcessState coreState = report.getGenericReport() == null ? ProcessState.EMPTY : state;
          createOrUpdate(curator, report.getDatasetKey(), dataset.getType() == DatasetType.CHECKLIST ? PROCESS_STATE_CHECKLIST : PROCESS_STATE_SAMPLE, coreState);

          // update occurrence status
          createOrUpdate(curator, report.getDatasetKey(), PROCESS_STATE_OCCURRENCE, occurrenceState);
          break;
        case METADATA:
          // for metadata only dataset this is the last step
          // explicitly declare that no content is expected so the CoordinatorCleanup can pick it up.
          createOrUpdate(curator, report.getDatasetKey(), PROCESS_STATE_OCCURRENCE, ProcessState.EMPTY);
          createOrUpdate(curator, report.getDatasetKey(), PROCESS_STATE_CHECKLIST, ProcessState.EMPTY);
          createOrUpdate(curator, report.getDatasetKey(), PROCESS_STATE_SAMPLE, ProcessState.EMPTY);
          LOG.info("Marked metadata-only dataset as empty [{}]", datasetKey);
          break;
        default:
          LOG.error("Can't updateProcessState dataset [{}]: unknown type -> {}", datasetKey, dataset.getType());
      }
    }
  }
}
