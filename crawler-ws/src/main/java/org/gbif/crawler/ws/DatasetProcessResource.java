package org.gbif.crawler.ws;

import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.service.crawler.DatasetProcessService;
import org.gbif.ws.util.ExtraMediaTypes;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;


@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
@Path("dataset/process")
public class DatasetProcessResource implements DatasetProcessService {

  private final DatasetProcessService service;

  @Inject
  public DatasetProcessResource(DatasetProcessService service) {
    this.service = service;
  }

  @GET
  @Path("detail/{key}")
  @Override
  public DatasetProcessStatus getDatasetProcessStatus(@PathParam("key") UUID uuid) {
    return service.getDatasetProcessStatus(uuid);
  }

  @GET
  @Path("running")
  @Override
  public Set<DatasetProcessStatus> getRunningDatasetProcesses() {
    return service.getRunningDatasetProcesses();
  }

  @GET
  @Path("xml/pending")
  @Override
  public List<DatasetProcessStatus> getPendingXmlDatasetProcesses() {
    return service.getPendingXmlDatasetProcesses();
  }

  @GET
  @Path("dwca/pending")
  @Override
  public List<DatasetProcessStatus> getPendingDwcaDatasetProcesses() {
    return service.getPendingDwcaDatasetProcesses();
  }

}
