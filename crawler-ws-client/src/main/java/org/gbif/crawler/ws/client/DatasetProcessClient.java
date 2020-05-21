package org.gbif.crawler.ws.client;

import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.service.crawler.DatasetProcessService;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Client-side implementation to the crawler service.
 */
@RequestMapping("dataset/process")
public interface DatasetProcessClient extends DatasetProcessService {

  @RequestMapping(method = RequestMethod.GET, value = "detail", produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  @Override
  DatasetProcessStatus getDatasetProcessStatus(UUID datasetKey);

  @RequestMapping(method = RequestMethod.GET, value = "running", produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  @Override
  Set<DatasetProcessStatus> getRunningDatasetProcesses();

  @RequestMapping(method = RequestMethod.GET, value = "xml/pending", produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  @Override
  List<DatasetProcessStatus> getPendingXmlDatasetProcesses();

  @RequestMapping(method = RequestMethod.GET, value = "dwca/pending", produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  @Override
  List<DatasetProcessStatus> getPendingDwcaDatasetProcesses();

}
