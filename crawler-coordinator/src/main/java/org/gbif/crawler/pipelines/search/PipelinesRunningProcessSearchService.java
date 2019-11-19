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

/** Simple embedded search service to retrieve pipeline processes by dataset title. */
public class PipelinesRunningProcessSearchService implements Closeable {

  private static final String KEY_FIELD = "key";
  private static final String DATASET_TITLE_FIELD = "datasetTitle";
  private static final String STATUS_FIELD = "status";
  private static final String STEP_FIELD = "step";
  private static final String STEP_STATUS_FIELD_POST = "_step_status";

  private final SimpleSearchIndex datasetSimpleSearchIndex;

  private static final int MAX_PAGE_SIZE = 100;

  /**
   * Creates a search index at the specified path, contents of the directory will be removed.
   *
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
    return stepType.name() + STEP_STATUS_FIELD_POST;
  }

  private static List<String> fromSearchResult(SimpleSearchIndex.SearchResult searchResult) {
    return searchResult.getResults().stream()
        .map(m -> m.get(KEY_FIELD))
        .collect(Collectors.toList());
  }

  /** Adds a {@link PipelineProcess} to the search index. */
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

      datasetSimpleSearchIndex.index(doc);
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Deletes a {@link PipelineProcess} from the search index. */
  public void delete(String crawlId) {
    try {
      datasetSimpleSearchIndex.delete(KEY_FIELD, crawlId);
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by dataset title */
  public List<String> searchByDatasetKey(String datasetKeyQ, int pageNumber, int pageSize) {
    try {
      return fromSearchResult(
          datasetSimpleSearchIndex.search(
              KEY_FIELD,
              Objects.isNull(datasetKeyQ) ? "" : datasetKeyQ.trim() + "_*",
              pageNumber,
              Math.min(MAX_PAGE_SIZE, pageSize)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by dataset title */
  public List<String> searchByDatasetKeyAndAttempt(
      String datasetKeyQ, int attempt, int pageNumber, int pageSize) {
    try {
      return fromSearchResult(
          datasetSimpleSearchIndex.search(
              KEY_FIELD,
              Objects.requireNonNull(datasetKeyQ).trim() + "_" + attempt,
              pageNumber,
              Math.min(MAX_PAGE_SIZE, pageSize)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by dataset title */
  public List<String> searchByDatasetTitle(String datasetTitleQ, int pageNumber, int pageSize) {
    try {
      return fromSearchResult(
          datasetSimpleSearchIndex.search(
              DATASET_TITLE_FIELD,
              Objects.isNull(datasetTitleQ) ? "" : datasetTitleQ.trim(),
              pageNumber,
              Math.min(MAX_PAGE_SIZE, pageSize)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by step status */
  public List<String> searchByStepStatus(
      StepType stepType, PipelineStep.Status status, int pageNumber, int pageSize) {
    try {
      return fromSearchResult(
          datasetSimpleSearchIndex.termSearch(
              toStepStatusField(stepType),
              status.name().toLowerCase(),
              pageNumber,
              Math.min(MAX_PAGE_SIZE, pageSize)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by step status: i.e. if any step matches the status */
  public List<String> searchByStatus(PipelineStep.Status status, int pageNumber, int pageSize) {
    try {
      return fromSearchResult(
          datasetSimpleSearchIndex.termSearch(
              STATUS_FIELD,
              status.name().toLowerCase(),
              pageNumber,
              Math.min(MAX_PAGE_SIZE, pageSize)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Pageable search by step status: i.e. if any step matches the status */
  public List<String> searchByStep(StepType stepType, int pageNumber, int pageSize) {
    try {
      return fromSearchResult(
          datasetSimpleSearchIndex.termSearch(
              STEP_FIELD,
              stepType.name().toLowerCase(),
              pageNumber,
              Math.min(MAX_PAGE_SIZE, pageSize)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  @Override
  public void close() {
    datasetSimpleSearchIndex.close();
  }
}
