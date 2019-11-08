package org.gbif.crawler.pipelines.search;

import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.sun.xml.internal.xsom.impl.scd.Step;
import org.cache2k.Cache;
import org.cache2k.CacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple embedded search service to retrieve pipeline processes by dataset title.
 */
public class PipelinesRunningProcessSearchService implements Closeable  {

  private static final Logger LOG = LoggerFactory.getLogger(PipelinesRunningProcessSearchService.class);

  private static final String KEY_FIELD = "key";
  private static final String DATASET_TITLE_FIELD = "datasetTitle";
  private static final String STEP_STATUS_FIELD_POST = "_step_status";
  private static final int MAX_PAGE_SIZE = 100;

  private final SimpleSearchIndex datasetSimpleSearchIndex;

  private Cache<String, PipelineProcess> dataCache;

  /**
   * Creates a search index at the specified path, contents of the directory will be removed.
   * @param path directory to store the index
   */
  public PipelinesRunningProcessSearchService(String path) {
    try {
      datasetSimpleSearchIndex = SimpleSearchIndex.create(path);
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }


  private static String getKey(PipelineProcess pipelineProcess) {
    return pipelineProcess.getDatasetKey().toString() + '_' + pipelineProcess.getAttempt();
  }

  private static String toStepStatusField(StepType stepType) {
    return stepType.name() +  STEP_STATUS_FIELD_POST;
  }

  private static PipelineProcess fromDocKey(String docKey) {
    String[] datasetKeyAttempt = docKey.split("_");
    PipelineProcess pipelineProcess = new PipelineProcess();
    pipelineProcess.setDatasetKey(UUID.fromString(datasetKeyAttempt[0]));
    pipelineProcess.setAttempt(Integer.parseInt(datasetKeyAttempt[1]));
    return pipelineProcess;
  }

  private PipelineProcessSearchResult fromCache(SimpleSearchIndex.SearchResult searchResult, Cache<String, PipelineProcess> dataCache) {
    List<PipelineProcess> results = searchResult.getResults().stream()
                                      .map(doc -> {
                                        //An instance is created, relies on the equals method of PipelineProcess which only takes into account the datasetKey and the attempt
                                         PipelineProcess pipelineProcessFound = fromDocKey(doc.get(KEY_FIELD));
                                        return StreamSupport.stream(dataCache.entries().spliterator(), false)
                                          .map(CacheEntry::getValue)
                                          .filter(pipelineProcess -> pipelineProcess.equals(pipelineProcessFound))
                                          .collect(Collectors.toList());
                                      }).flatMap(List::stream).collect(Collectors.toList());
    return new PipelineProcessSearchResult(searchResult.getTotalHits(), results);
  }

  /** Adds a {@link PipelineProcess} to the search index.*/
  public void index(PipelineProcess pipelineProcess) {
    try {
      Map<String, String> doc = new HashMap<>();
      String key = getKey(pipelineProcess);
      doc.put(KEY_FIELD, key);
      doc.put(DATASET_TITLE_FIELD, pipelineProcess.getDatasetTitle());
      if(Objects.nonNull(pipelineProcess.getSteps())) {
        pipelineProcess.getSteps().forEach(step -> {
          doc.put(toStepStatusField(step.getType()), step.getState().name());
        });
      }
      datasetSimpleSearchIndex.upsert(KEY_FIELD, key, doc);
    } catch (IOException ex) {
      LOG.error("PipelineProcess {}  can't be added to search index", pipelineProcess, ex);
      throw new IllegalStateException(ex);
    }
  }

  /** Deletes a {@link PipelineProcess} from the search index.*/
  public void delete(PipelineProcess pipelineProcess) {
    try {
      datasetSimpleSearchIndex.delete(KEY_FIELD, getKey(pipelineProcess));
    } catch (IOException ex) {
      LOG.error("PipelineProcess {}  can't be removed from the search index", pipelineProcess, ex);
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by dataset title */
  public PipelineProcessSearchResult searchByDatasetTitle(String datasetTitleQ, int pageNumber, int pageSize, Cache<String, PipelineProcess> dataCache) {
    try {
      return fromCache(datasetSimpleSearchIndex.search(DATASET_TITLE_FIELD, Objects.isNull(datasetTitleQ)? "" : datasetTitleQ.trim(), pageNumber, Math.min(MAX_PAGE_SIZE, pageSize)), dataCache);
    } catch (IOException ex) {
      LOG.error("Error searching for dataset title {}", datasetTitleQ, ex);
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by step status*/
  public PipelineProcessSearchResult searchByStepStatus(StepType stepType, PipelineStep.Status status, int pageNumber, int pageSize, Cache<String, PipelineProcess> dataCache) {
    try {
      return fromCache(datasetSimpleSearchIndex.termSearch(toStepStatusField(stepType), status.name().toLowerCase(), pageNumber, Math.min(MAX_PAGE_SIZE, pageSize)), dataCache);
    } catch (IOException ex) {
      LOG.error("Error searching for step {} with status {}", stepType, status, ex);
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by step status: i.e. if any step matches the status*/
  public PipelineProcessSearchResult searchByStatus(PipelineStep.Status status, int pageNumber, int pageSize, Cache<String, PipelineProcess> dataCache) {
    try {
      Map<String,String> searchTerms = Arrays.stream(StepType.values()).collect(Collectors.toMap(PipelinesRunningProcessSearchService::toStepStatusField, step -> status.name().toLowerCase()));
      return fromCache(datasetSimpleSearchIndex.multiTermSearch(searchTerms, pageNumber, Math.min(MAX_PAGE_SIZE, pageSize)), dataCache);
    } catch (IOException ex) {
      LOG.error("Error searching for status {}", status, ex);
      throw new IllegalStateException(ex);
    }
  }

  @Override
  public void close() {
    datasetSimpleSearchIndex.close();
  }

  /**
   * Search results.
   */
  public static final class PipelineProcessSearchResult {

    private final long totalHits;

    private final List<PipelineProcess> results;

    PipelineProcessSearchResult(long totalHits, List<PipelineProcess> results) {
      this.totalHits = totalHits;
      this.results = results;
    }

    /**@return the total/global number of results*/
    public long getTotalHits() {
      return totalHits;
    }

    /**@return PipelineProcess search results*/
    public List<PipelineProcess> getResults() {
      return results;
    }
  }

}
