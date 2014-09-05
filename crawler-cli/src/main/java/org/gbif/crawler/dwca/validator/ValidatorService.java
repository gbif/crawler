package org.gbif.crawler.dwca.validator;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcaDownloadFinishedMessage;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.dwca.DwcaService;
import org.gbif.crawler.dwca.LenientArchiveFactory;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.UnsupportedArchiveException;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_FRAGMENTED_ERROR;

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
    listener.listen(config.queueName,
                    config.poolSize,
                    new DwcaDownloadFinishedMessageCallback(datasetService,
                                                            config.archiveRepository,
                                                            publisher,
                                                            curator));
  }

  private static class DwcaDownloadFinishedMessageCallback extends AbstractMessageCallback<DwcaDownloadFinishedMessage> {

    private final DatasetService datasetService;
    private final File archiveRepository;
    private final MessagePublisher publisher;
    private final CuratorFramework curator;

    private final Counter messageCount = Metrics.newCounter(ValidatorService.class, "messageCount");
    private final Counter failedValidations = Metrics.newCounter(ValidatorService.class, "failedValidations");

    private DwcaDownloadFinishedMessageCallback(
      DatasetService datasetService, File archiveRepository, MessagePublisher publisher, CuratorFramework curator
    ) {
      this.datasetService = datasetService;
      this.archiveRepository = archiveRepository;
      this.publisher = publisher;
      this.curator = curator;
    }

    @Override
    public void handleMessage(DwcaDownloadFinishedMessage message) {
      messageCount.inc();

      final UUID uuid = message.getDatasetUuid();

      if (!message.isModified()) {
        LOG.info("DwC-A for dataset [{}] was not modified, skipping validation", uuid);
        return;
      }

      LOG.info("Now validating DwC-A for dataset [{}]", uuid);
      Dataset dataset = datasetService.get(uuid);
      if (dataset == null) {
        // exception, we don't know this dataset
        throw new IllegalArgumentException("The requested dataset " + uuid + " is not registered");
      }

      DwcaValidationReport validationReport;
      try {
        Archive archive = LenientArchiveFactory.openArchive(new File(archiveRepository, uuid.toString()));
        validationReport = DwcaValidator.validate(dataset, archive);

      } catch (UnsupportedArchiveException e) {
        LOG.warn("Invalid Dwc archive for dataset {}", uuid, e);
        validationReport = new DwcaValidationReport(uuid, "Invalid Dwc archive");

      } catch (IOException e) {
        LOG.warn("IOException when reading dwc archive for dataset {}", uuid, e);
        validationReport = new DwcaValidationReport(uuid, "IOException when reading dwc archive");
      }

      if (!validationReport.isValid()) {
        failedValidations.inc();

        // If invalid we also need to update ZooKeeper
        RetryPolicy retryPolicy = new RetryNTimes(5, 1000);

        String path = CrawlerNodePaths.getCrawlInfoPath(uuid, PAGES_FRAGMENTED_ERROR);
        DistributedAtomicLong dal = new DistributedAtomicLong(curator, path, retryPolicy);
        try {
          dal.trySet(1L);
        } catch (Exception e) {
          LOG.error("Failed to update counter for successful DwC-A fragmenting", e);
        }
      }

      LOG.info("Finished validating DwC-A for dataset [{}], valid? is [{}]. Full report [{}]",
               uuid, validationReport.isValid(), validationReport);

      // send validation finished message
      try {
        publisher.send(new DwcaValidationFinishedMessage(uuid,
                                                         dataset.getType(),
                                                         message.getSource(),
                                                         message.getAttempt(),
                                                         validationReport));
      } catch (IOException e) {
        LOG.warn("Failed to send validation finished message for dataset {}", uuid, e);
      }
    }
  }

}
