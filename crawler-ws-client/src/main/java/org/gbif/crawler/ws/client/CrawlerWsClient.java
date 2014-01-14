package org.gbif.crawler.ws.client;

import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.service.crawler.DatasetProcessService;
import org.gbif.ws.client.BaseWsClient;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;

/**
 * Client-side implementation to the crawler service.
 */
public class CrawlerWsClient extends BaseWsClient implements DatasetProcessService {

  private static final GenericType<Set<DatasetProcessStatus>> CRAWL_SET_TYPE =
    new GenericType<Set<DatasetProcessStatus>>() {};

  private static final GenericType<List<DatasetProcessStatus>> CRAWL_LIST_TYPE =
    new GenericType<List<DatasetProcessStatus>>() {};

  private static final String DATASET_PATH = "dataset/process";

  @Inject
  public CrawlerWsClient(WebResource resource) {
    super(resource.path(DATASET_PATH));
  }


  @Override
  public DatasetProcessStatus getDatasetProcessStatus(UUID datasetKey) {
    return resource.path("detail")
      .path(datasetKey.toString())
      .type(MediaType.APPLICATION_JSON)
      .get(DatasetProcessStatus.class);
  }

  @Override
  public Set<DatasetProcessStatus> getRunningDatasetProcesses() {
    return resource.path("running").type(MediaType.APPLICATION_JSON).get(CRAWL_SET_TYPE);
  }

  @Override
  public List<DatasetProcessStatus> getPendingXmlDatasetProcesses() {
    return resource.path("xml/pending").type(MediaType.APPLICATION_JSON).get(CRAWL_LIST_TYPE);
  }

  @Override
  public List<DatasetProcessStatus> getPendingDwcaDatasetProcesses() {
    return resource.path("dwca/pending").type(MediaType.APPLICATION_JSON).get(CRAWL_LIST_TYPE);
  }

}
