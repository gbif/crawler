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
import org.gbif.crawler.dwca.DwcaService;
import org.gbif.crawler.dwca.downloader.DwcaCrawlConsumer;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;
import org.gbif.dwca.io.UnsupportedArchiveException;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
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
      new DwcaDownloadFinishedMessageCallback(datasetService, config.archiveRepository, publisher, curator));
  }

  private static class DwcaDownloadFinishedMessageCallback
    extends AbstractMessageCallback<DwcaDownloadFinishedMessage> {

    private final DatasetService datasetService;
    private final File archiveRepository;
    private final MessagePublisher publisher;
    private final CuratorFramework curator;

    private final Counter messageCount = Metrics.newCounter(ValidatorService.class, "messageCount");
    private final Counter failedValidations = Metrics.newCounter(ValidatorService.class, "failedValidations");

    private DwcaDownloadFinishedMessageCallback(DatasetService datasetService, File archiveRepository,
      MessagePublisher publisher, CuratorFramework curator) {
      this.datasetService = datasetService;
      this.archiveRepository = archiveRepository;
      this.publisher = publisher;
      this.curator = curator;
    }

    @Override
    public void handleMessage(DwcaDownloadFinishedMessage message) {
      messageCount.inc();

      final UUID datasetKey = message.getDatasetUuid();
      try (MDC.MDCCloseable closeable = MDC.putCloseable("datasetKey", datasetKey.toString())) {

        LOG.info("Now validating DwC-A for dataset [{}]", datasetKey);
        Dataset dataset = datasetService.get(datasetKey);
        if (dataset == null) {
          // exception, we don't know this dataset
          throw new IllegalArgumentException("The requested dataset " + datasetKey + " is not registered");
        }

        DwcaValidationReport validationReport;
        final File dwcaFile = new File(archiveRepository, datasetKey + DwcaCrawlConsumer.DWCA_SUFFIX);
        final File archiveDir = new File(archiveRepository, datasetKey.toString());
        try {
          Archive archive = ArchiveFactory.openArchive(dwcaFile, archiveDir);
          validationReport = DwcaValidator.validate(dataset, archive);

        } catch (UnsupportedArchiveException e) {
          LOG.warn("Invalid DwC archive for dataset [{}]", datasetKey, e);
          validationReport = new DwcaValidationReport(datasetKey, "Invalid Dwc archive");

        } catch (IOException e) {
          LOG.error("IOException when reading DwC archive for dataset [{}]", datasetKey, e);
          validationReport = new DwcaValidationReport(datasetKey, "IOException when reading DwC archive");

        } catch (RuntimeException e) {
          LOG.error("Unknown error when reading DwC archive for dataset [{}]", datasetKey, e);
          validationReport = new DwcaValidationReport(datasetKey, "Unexpected error when reading DwC archive: " + e.getMessage());
        }

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
                  validationReport));
        } catch (IOException e) {
          LOG.error("Failed to send validation finished message for dataset [{}]", datasetKey, e);
        }
      }
    }

    /**
     * For existing dataset types (that contains data) this sets the process state as given.
     * For METADATA, this method will simply return. DwcaMetasyncService will handle them.
     *
     */
    private void updateProcessState(Dataset dataset, DwcaValidationReport report, ProcessState state) {

      switch(dataset.getType()){
        case OCCURRENCE:
          createOrUpdate(curator, report.getDatasetKey(), PROCESS_STATE_OCCURRENCE, state);
          break;
        case CHECKLIST:
        case SAMPLING_EVENT:
          // we might have a mixed dataset with taxa or events and optionally also occurrences

          // update core status
          // if there is no report, we record empty, otherwise we record the given state
          ProcessState coreState = report.getGenericReport() == null ? ProcessState.EMPTY : state;
          createOrUpdate(curator, report.getDatasetKey(), dataset.getType() == DatasetType.CHECKLIST ? PROCESS_STATE_CHECKLIST : PROCESS_STATE_SAMPLE, coreState);

          // update occurrence status
          if (report.getOccurrenceReport() == null || report.getOccurrenceReport().getCheckedRecords() == 0) {
            createOrUpdate(curator, report.getDatasetKey(), PROCESS_STATE_OCCURRENCE, ProcessState.EMPTY);
          } else {
            createOrUpdate(curator, report.getDatasetKey(), PROCESS_STATE_OCCURRENCE, state);
          }
          break;
        case METADATA:
          // no-op, DwcaMetasyncService will set PROCESS_STATE_OCCURRENCE and PROCESS_STATE_CHECKLIST to EMPTY
          break;
        default:
          LOG.error("Can't updateProcessState dataset [{}]: unknown type -> {}", datasetKey, dataset.getType());
      }
    }
  }
}
