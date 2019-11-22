package org.gbif.crawler.pipelines.search;

import org.gbif.api.model.pipelines.PipelineProcess;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

/** Simple embedded search service to retrieve pipeline processes by dataset title. */
public class PipelinesRunningProcessSearchService implements Closeable {

  private static final String KEY_FIELD = "key";
  private static final String DATASET_TITLE_FIELD = "datasetTitle";
  private static final String DATASET_KEY_FIELD = "datasetKey";
  private static final String STATUS_FIELD = "status";
  private static final String STEP_FIELD = "step";

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

  /** Adds a {@link PipelineProcess} to the search index. */
  public void index(PipelineProcess pipelineProcess) {
    try {
      datasetSimpleSearchIndex.index(createDocument(pipelineProcess));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  public void update(PipelineProcess pipelineProcess) {
    try {
      datasetSimpleSearchIndex.update(
          KEY_FIELD, getKey(pipelineProcess), createDocument(pipelineProcess).getFields());
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

  /** Pageable search by step status */
  public List<String> search(
      SearchParams searchParams, int pageNumber, int pageSize) {

    Map<String, String> searchQueries =
        searchParams
            .getDatasetTitle()
            .map(title -> Collections.singletonMap(DATASET_TITLE_FIELD, title.trim() + "*"))
            .orElse(new HashMap<>());

    Map<String, Set<String>> termQueries = new HashMap<>();
    searchParams
        .getDatasetKey()
        .ifPresent(
            key -> termQueries.put(DATASET_KEY_FIELD, Collections.singleton(key.toString())));

    if (searchParams.getStepTypes() != null && !searchParams.getStepTypes().isEmpty()) {
      termQueries.put(
          STEP_FIELD,
          searchParams.getStepTypes().stream().map(StepType::name).collect(Collectors.toSet()));
    }

    if (searchParams.getStatuses() != null && !searchParams.getStatuses().isEmpty()) {
      termQueries.put(
          STATUS_FIELD,
          searchParams.getStatuses().stream()
              .map(PipelineStep.Status::name)
              .collect(Collectors.toSet()));
    }

    try {
      return fromSearchResult(
          datasetSimpleSearchIndex.multiTermSearch(
              termQueries, searchQueries, pageNumber, Math.min(MAX_PAGE_SIZE, pageSize)));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  private static String getKey(PipelineProcess pipelineProcess) {
    return pipelineProcess.getDatasetKey().toString() + '_' + pipelineProcess.getAttempt();
  }

  private static List<String> fromSearchResult(SimpleSearchIndex.SearchResult searchResult) {
    return searchResult.getResults().stream()
        .map(m -> m.get(KEY_FIELD))
        .collect(Collectors.toList());
  }

  private Document createDocument(PipelineProcess pipelineProcess) {
    Document doc = new Document();

    // add key
    doc.add(new StringField(KEY_FIELD, getKey(pipelineProcess), Field.Store.YES));

    if (pipelineProcess.getDatasetKey() != null) {
      doc.add(
          new StringField(
              DATASET_KEY_FIELD, pipelineProcess.getDatasetKey().toString(), Field.Store.NO));
    }

    if (!Strings.isNullOrEmpty(pipelineProcess.getDatasetTitle())) {
      doc.add(
          new TextField(DATASET_TITLE_FIELD, pipelineProcess.getDatasetTitle(), Field.Store.NO));
    }

    if (Objects.nonNull(pipelineProcess.getSteps())) {
      pipelineProcess
          .getSteps()
          .forEach(
              step -> {
                if (step.getState() != null) {
                  doc.add(new StringField(STATUS_FIELD, step.getState().name(), Field.Store.NO));
                }
                if (step.getType() != null) {
                  doc.add(new StringField(STEP_FIELD, step.getType().name(), Field.Store.NO));
                }
              });
    }

    return doc;
  }

  @Override
  public void close() {
    datasetSimpleSearchIndex.close();
  }
}
