package org.gbif.crawler.pipelines;

import java.util.Set;

/**
 * The public interface of the Pipelines monitoring service
 */
public interface PipelinesProcessService {

  Set<PipelinesProcessStatus> getRunningPipelinesProcesses();

  PipelinesProcessStatus getRunningPipelinesProcess(String crawlId);

  Set<PipelinesProcessStatus> getPipelinesProcessesByDatasetKey(String datasetKey);

}
