package org.gbif.crawler.dwca.metasync;

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.TagName;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcaMetasyncFinishedMessage;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.dwca.DwcaConfiguration;
import org.gbif.crawler.dwca.DwcaService;
import org.gbif.dwc.Archive;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Maps;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.gbif.dwc.DwcFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_FRAGMENTED_ERROR;

/**
 * Service that listens to DwcaValidationFinishedMessages and puts found metadata documents into the metadata
 * repository thereby updating the registered datasets information. Is aware of constituent datasets within an
 * archive and therefore knows how to process Catalogue of Life GSD information.
 */
public class DwcaMetasyncService extends DwcaService {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaMetasyncService.class);
  private final DwcaMetasyncConfiguration configuration;

  public DwcaMetasyncService(DwcaMetasyncConfiguration configuration) {
    super(configuration);
    this.configuration = configuration;
  }

  @Override
  protected void bindListeners() throws IOException {
    CuratorFramework curator = configuration.zooKeeper.getCuratorFramework();

    // listen to DwcaValidationFinishedMessage messages
    listener.listen("dwca-metasync", config.poolSize,
      new DwcaValidationFinishedMessageCallback(datasetService, config.archiveRepository, publisher, curator));
  }

  private static class DwcaValidationFinishedMessageCallback
    extends AbstractMessageCallback<DwcaValidationFinishedMessage> {

    private final DatasetService datasetService;
    private final File archiveRepository;
    private final MessagePublisher publisher;
    private final CuratorFramework curator;

    private final Counter messageCount = Metrics.newCounter(DwcaMetasyncService.class, "messageCount");
    private final Counter datasetsUpdated = Metrics.newCounter(DwcaMetasyncService.class, "datasetsUpdated");
    private final Counter constituentsAdded = Metrics.newCounter(DwcaMetasyncService.class, "constituentsAdded");
    private final Counter constituentsDeleted = Metrics.newCounter(DwcaMetasyncService.class, "constituentsDeleted");
    private final Counter constituentsUpdated = Metrics.newCounter(DwcaMetasyncService.class, "constituentsUpdated");

    private DwcaValidationFinishedMessageCallback(DatasetService datasetService, File archiveRepository,
      MessagePublisher publisher, CuratorFramework curator) {
      this.archiveRepository = archiveRepository;
      this.datasetService = datasetService;
      this.publisher = publisher;
      this.curator = curator;
    }

    @Override
    public void handleMessage(DwcaValidationFinishedMessage message) {
      MDC.put("datasetKey", message.getDatasetUuid().toString());
      messageCount.inc();
      UUID uuid = message.getDatasetUuid();
      LOG.info("Updating metadata from DwC-A for dataset [{}]", uuid);

      try {
        handleMessageInternal(message, uuid);
      } catch (Exception e) {
        LOG.error("Exception caught during metasyncing DwC-A [{}]", uuid, e);
        updateZookeeper(uuid);
      } finally {
        MDC.remove("datasetKey");
      }
    }

    private void handleMessageInternal(DwcaValidationFinishedMessage message, UUID datasetKey) throws IOException {
      // https://github.com/gbif/portal-feedback/issues/2138
      // The DwcaValidation doesn't check the metadata anyway, so it's OK to continue.
      if (!message.getValidationReport().isValid()) {
        LOG.warn("Invalid DwC-A for dataset [{}], attempting to process metadata anyway", datasetKey);
      }

      Dataset dataset = datasetService.get(datasetKey);
      if (dataset == null) {
        // exception, we don't know this dataset
        throw new IllegalArgumentException("The requested dataset " + message.getDatasetUuid() + " is not registered");
      }

      // Metadata-only datasets are just a file, no DWCA.
      Archive archive;
      File metaFile;
      if (DatasetType.METADATA == dataset.getType()) {
        archive = null;
        metaFile = new File(new File(archiveRepository, datasetKey.toString()), DwcaConfiguration.METADATA_FILE);
      } else {
        archive = DwcFiles.fromLocation(new File(archiveRepository, datasetKey.toString()).toPath());
        metaFile = archive.getMetadataLocationFile();
      }

      if (metaFile != null && metaFile.exists()) {
        // metadata found, put into repository thereby updating the dataset
        setMetaDocument(metaFile, datasetKey);
        datasetsUpdated.inc();
      }

      // Metadata-only datasets can't have constituents
      Map<String, UUID> constituents;
      if (DatasetType.METADATA == dataset.getType()) {
        constituents = new HashMap();
      } else {
        // process dataset constituents
        constituents = processConstituents(dataset, archive);
      }

      LOG.info("Finished updating metadata from DwC-A for dataset [{}]", datasetKey);

      if (Platform.OCCURRENCE.equivalent(message.getPlatform())) {
        if (message.getValidationReport().isValid()) {
          // send success message
          publisher.send(new DwcaMetasyncFinishedMessage(datasetKey, dataset.getType(), message.getSource(),
              message.getAttempt(), constituents, message.getValidationReport(),
              message.getPlatform()), true);
        } else {
          LOG.warn("Metadata processed, but not sending completion message because the archive is invalid.");
        }
      }
    }

    private Map<String, UUID> processConstituents(Dataset parent, Archive archive) {
      Map<String, UUID> constituents = Maps.newHashMap();
      // we don't expect to reach more than a few hundred constituents - so ignore paging here by using a 2000 p size
      Pageable page = new PagingRequest(0, 2000);
      List<Dataset> existingConstituents = datasetService.listConstituents(parent.getKey(), page).getResults();
      LOG.info("{} existing constituents registered for {}", existingConstituents.size(), parent.getKey());
      Map<String, File> archiveConstituents = archive.getConstituentMetadata();
      LOG.info("{} constituents metadata found in archive {}", archiveConstituents.size(), parent.getKey());

      // go through each existing constituent and update its metadata or delete it from the registry
      for (Dataset constituent : existingConstituents) {
        try {
          // we keep the datasetID as a tag, get it
          String datasetId = getTagValue(constituent.getKey(), TagName.DATASET_ID);
          if (datasetId == null) {
            LOG.warn("Existing registered constituent {} found without a tagged datasetID. "
                     + "Please adjust manually as we will likely be creating a new constituent dataset",
              constituent.getKey());

          } else {
            // do we still have this constituent in the archive?
            if (archiveConstituents.containsKey(datasetId)) {
              // remove from archive map so we have only the new ones at the end
              File metaFile = archiveConstituents.remove(datasetId);
              setMetaDocument(metaFile, constituent.getKey());
              constituentsUpdated.inc();
              constituents.put(datasetId, constituent.getKey());

            } else {
              // constituent has been removed. Delete in registry
              datasetService.delete(constituent.getKey());
              constituentsDeleted.inc();
              LOG.info("Existing constituent with ID={} deleted, not found in archive anymore", datasetId);
            }
          }
        } catch (FileNotFoundException e) {
          LOG.error("Failed to read archive constituent metadata file for already registered dataset {}",
            constituent.getKey(), e);
        } catch (IllegalArgumentException e) {
          LOG.error("Constituent dataset with UUID key expected, but got [{}]", parent.getKey(), e);
        }
      }

      // now see if there are any new constituents left and create them
      for (Map.Entry<String, File> constituent : archiveConstituents.entrySet()) {
        String datasetId = constituent.getKey();
        File metaFile = constituent.getValue();
        UUID constituentKey = addNewConstituent(parent, parent.getKey(), datasetId, metaFile);
        constituents.put(datasetId, constituentKey);
      }

      return constituents;
    }

    private boolean setMetaDocument(File metaDoc, UUID datasetKey) throws FileNotFoundException {
      try (InputStream stream = new FileInputStream(metaDoc)){
        datasetService.insertMetadata(datasetKey, stream);
        LOG.info("Metadata document inserted from file {}", metaDoc);
        return true;
      } catch (IllegalArgumentException e) {
        LOG.warn("Metadata document {} for dataset {} not understood", metaDoc.getAbsolutePath(), datasetKey, e);
      } catch (Exception e) {
        LOG.error("Failed to upload metadata file {} for dataset {}", metaDoc.getAbsoluteFile(), datasetKey, e);
      }
      return false;
    }

    /**
     * Creates a new constituent dataset of the same type as the parent in the registry which will be linked to the
     * parent dataset and tagged with a datasetID.
     * The metadata file will be uploaded into the repository and its information used to update the constituent
     * dataset.
     *
     * @return the newly created constituent dataset key
     */
    private UUID addNewConstituent(Dataset parent, UUID parentKey, String datasetID, File metaFile) {
      Dataset constituent = new Dataset();
      // use temporary required title that will get overwritten when submitting the metadata file
      constituent.setTitle("Constituent " + datasetID + " of " + parent.getTitle());
      constituent.setParentDatasetKey(parentKey);
      constituent.setPublishingOrganizationKey(parent.getPublishingOrganizationKey());
      constituent.setInstallationKey(parent.getInstallationKey());
      constituent.setType(parent.getType());
      constituent.setSubtype(parent.getSubtype());

      try {
        UUID key = datasetService.create(constituent);
        LOG.info("Created new constituent {} with key {} for dataset {}", datasetID, key, parentKey);
        constituentsAdded.inc();

        MachineTag idTag = MachineTag
          .newInstance(TagName.DATASET_ID.getNamespace().getNamespace(), TagName.DATASET_ID.getName(), datasetID);
        datasetService.addMachineTag(key, idTag);

        setMetaDocument(metaFile, key);
        return key;

      } catch (FileNotFoundException e) {
        LOG.warn("Failed to upload metadata file for new constituent {} of dataset {}", datasetID, parentKey, e);

      } catch (Exception e) {
        LOG.error("Failed to create new constituent {} for dataset {}", datasetID, parentKey, e);
      }

      return null;
    }

    /**
     * Convenience method to retrieve a single tag for a given name enum or null.
     * If there are multiple tags with the same name only the first is returned.
     *
     * @param name the name to look for
     */
    // TODO(lfrancke): I think this is bad behavior as it usually indicates something wrong in the registry if there are
    // multiple tags with the same name (when we expect only one) and we should rather fail fast and clean up
    private String getTagValue(UUID datasetKey, TagName name) {
      List<MachineTag> tags = datasetService.listMachineTags(datasetKey);
      for (MachineTag tag : tags) {
        if (tag.getNamespace().equals(name.getNamespace().getNamespace()) && name.getName().equals(tag.getName())) {
          return tag.getValue();
        }
      }
      return null;
    }

    private void updateZookeeper(UUID uuid) {
      RetryPolicy retryPolicy = new RetryNTimes(5, 1000);
      String path = CrawlerNodePaths.getCrawlInfoPath(uuid, PAGES_FRAGMENTED_ERROR);
      DistributedAtomicLong dal = new DistributedAtomicLong(curator, path, retryPolicy);
      try {
        dal.trySet(1L);
      } catch (Exception e) {
        LOG.error("Failed to update counter for successful DwC-A fragmenting", e);
      }
    }

  }

}
