package org.gbif.crawler.pipelines.search;

import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Simple embedded search service to retrieve pipeline processes by dataset title.
 */
public class PipelinesRunningProcessSearchService implements Closeable  {

  private static final ObjectMapper MAPPER = new ObjectMapper().disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);

  private static final String KEY_FIELD = "key";
  private static final String DATASET_TITLE_FIELD = "datasetTitle";
  private static final String STATUS_FIELD = "status";
  private static final String STEP_FIELD = "step";
  private static final String STEP_STATUS_FIELD_POST = "_step_status";
  private static final String JSON_FIELD = "json";

  private final SimpleSearchIndex datasetSimpleSearchIndex;

  public static final int MAX_PAGE_SIZE = 100;

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

  private static PipelineProcess fromDoc(Map<String,String> document) {
    try {
      return MAPPER.readValue(document.get(JSON_FIELD), PipelineProcess.class);
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  private PipelineProcessSearchResult fromCache(SimpleSearchIndex.SearchResult searchResult) {
    List<PipelineProcess> results = searchResult.getResults().stream()
                                      .map(PipelinesRunningProcessSearchService::fromDoc)
                                      .collect(Collectors.toList());
    return new PipelineProcessSearchResult(searchResult.getTotalHits(), results);
  }

  /** Adds a {@link PipelineProcess} to the search index.*/
  public void index(PipelineProcess pipelineProcess) {
    try {
      Map<String, String> doc = new HashMap<>();
      String key = getKey(pipelineProcess);
      doc.put(KEY_FIELD, key);

      if (pipelineProcess.getDatasetTitle() != null) {
        doc.put(DATASET_TITLE_FIELD, pipelineProcess.getDatasetTitle());
      }

      if (Objects.nonNull(pipelineProcess.getSteps())) {
        pipelineProcess
            .getSteps()
            .forEach(
                step -> {
                  if (step.getState() != null) {
                    doc.put(STATUS_FIELD, step.getState().name());
                    doc.put(toStepStatusField(step.getType()), step.getState().name());
                  }
                  doc.put(STEP_FIELD, step.getType().name());
                });
      }
      doc.put(JSON_FIELD, MAPPER.writeValueAsString(pipelineProcess));
      datasetSimpleSearchIndex.upsert(KEY_FIELD, key, doc);
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Deletes a {@link PipelineProcess} from the search index.*/
  public void delete(String crawlId) {
    try {
      datasetSimpleSearchIndex.delete(KEY_FIELD, crawlId);
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by dataset title */
  public PipelineProcessSearchResult listAll(int pageNumber, int pageSize) {
    try {
      return fromCache(datasetSimpleSearchIndex.listAll(pageNumber, Math.min(MAX_PAGE_SIZE, pageSize)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by dataset title */
  public PipelineProcessSearchResult searchByDatasetKey(String datasetKeyQ, int pageNumber, int pageSize) {
    try {
      return fromCache(datasetSimpleSearchIndex.search(KEY_FIELD, Objects.isNull(datasetKeyQ)? "" : datasetKeyQ.trim() + "_*", pageNumber, Math.min(MAX_PAGE_SIZE, pageSize)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by dataset title */
  public PipelineProcessSearchResult searchByDatasetKeyAndAttempt(String datasetKeyQ, int attempt, int pageNumber, int pageSize) {
    try {
      return fromCache(datasetSimpleSearchIndex.search(KEY_FIELD, Objects.requireNonNull(datasetKeyQ).trim() + "_" + attempt, pageNumber, Math.min(MAX_PAGE_SIZE, pageSize)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by dataset title */
  public PipelineProcessSearchResult searchByDatasetTitle(String datasetTitleQ, int pageNumber, int pageSize) {
    try {
      return fromCache(datasetSimpleSearchIndex.search(DATASET_TITLE_FIELD, Objects.isNull(datasetTitleQ)? "" : datasetTitleQ.trim(), pageNumber, Math.min(MAX_PAGE_SIZE, pageSize)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by step status*/
  public PipelineProcessSearchResult searchByStepStatus(StepType stepType, PipelineStep.Status status, int pageNumber, int pageSize) {
    try {
      return fromCache(datasetSimpleSearchIndex.termSearch(toStepStatusField(stepType), status.name().toLowerCase(), pageNumber, Math.min(MAX_PAGE_SIZE, pageSize)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by step status: i.e. if any step matches the status*/
  public PipelineProcessSearchResult searchByStatus(PipelineStep.Status status, int pageNumber, int pageSize) {
    try {
      return fromCache(datasetSimpleSearchIndex.termSearch(STATUS_FIELD, status.name().toLowerCase(), pageNumber, Math.min(MAX_PAGE_SIZE, pageSize)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by step status: i.e. if any step matches the status*/
  public PipelineProcessSearchResult searchByStep(StepType stepType, int pageNumber, int pageSize) {
    try {
      return fromCache(datasetSimpleSearchIndex.termSearch(STEP_FIELD, stepType.name().toLowerCase(), pageNumber, Math.min(MAX_PAGE_SIZE, pageSize)));
    } catch (IOException ex) {
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
