package org.gbif.crawler.ws;

import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.service.crawler.DatasetProcessService;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Primary
@RestController
@RequestMapping(value = "dataset/process", produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"})
public class DatasetProcessResource implements DatasetProcessService {

  private final DatasetProcessService service;

  @Autowired
  public DatasetProcessResource(DatasetProcessService service) {
    this.service = service;
  }

  @GetMapping("detail/{key}")
  @Override
  public DatasetProcessStatus getDatasetProcessStatus(@PathVariable("key") UUID uuid) {
    return service.getDatasetProcessStatus(uuid);
  }

  @GetMapping("running")
  @Override
  public Set<DatasetProcessStatus> getRunningDatasetProcesses() {
    return service.getRunningDatasetProcesses();
  }

  @GetMapping("xml/pending")
  @Override
  public List<DatasetProcessStatus> getPendingXmlDatasetProcesses() {
    return service.getPendingXmlDatasetProcesses();
  }

  @GetMapping("dwca/pending")
  @Override
  public List<DatasetProcessStatus> getPendingDwcaDatasetProcesses() {
    return service.getPendingDwcaDatasetProcesses();
  }

}
