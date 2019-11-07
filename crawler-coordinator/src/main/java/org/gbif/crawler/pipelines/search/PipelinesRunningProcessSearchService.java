package org.gbif.crawler.pipelines.search;

import org.gbif.api.model.pipelines.PipelineProcess;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

  private static PipelineProcess fromDocKey(String docKey) {
    String[] datasetKeyAttempt = docKey.split("_");
    PipelineProcess pipelineProcess = new PipelineProcess();
    pipelineProcess.setDatasetKey(UUID.fromString(datasetKeyAttempt[0]));
    pipelineProcess.setAttempt(Integer.parseInt(datasetKeyAttempt[1]));
    return pipelineProcess;
  }

  private List<PipelineProcess> fromCache(SimpleSearchIndex.SearchResult searchResult, Cache<String, PipelineProcess> dataCache) {
    return searchResult.getResults().stream()
      .map(doc -> {
        //An instance is created, relies on the equals method of PipelineProcess which only takes into account the datasetKey and the attempt
         PipelineProcess pipelineProcessFound = fromDocKey(doc.get(KEY_FIELD));
        return StreamSupport.stream(dataCache.entries().spliterator(), false)
          .map(CacheEntry::getValue)
          .filter(pipelineProcess -> pipelineProcess.equals(pipelineProcessFound))
          .collect(Collectors.toList());
      }).flatMap(List::stream).collect(Collectors.toList());
  }

  /** Adds a {@link PipelineProcess} to the search index.*/
  public void index(PipelineProcess pipelineProcess) {
    try {
      Map<String, String> doc = new HashMap<>();
      doc.put(KEY_FIELD, getKey(pipelineProcess));
      doc.put(DATASET_TITLE_FIELD, pipelineProcess.getDatasetTitle());
      datasetSimpleSearchIndex.index(doc);
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
  public List<PipelineProcess> searchByDatasetTitle(String datasetTitleQ, int pageNumber, int pageSize, Cache<String, PipelineProcess> dataCache) {
    try {
      return fromCache(datasetSimpleSearchIndex.search(DATASET_TITLE_FIELD, Objects.isNull(datasetTitleQ)? "" : datasetTitleQ.trim(), pageNumber, Math.min(MAX_PAGE_SIZE, pageSize)), dataCache);
    } catch (IOException ex) {
      LOG.error("Error searching for dataset title {}", datasetTitleQ, ex);
      throw new IllegalStateException(ex);
    }
  }

  @Override
  public void close() {
    datasetSimpleSearchIndex.close();
  }

}
