package org.gbif.crawler.pipelines.search;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.naming.directory.SearchResult;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Embeddable Search Index to perform simple full text search in a simple index of key value string elements.
 * Documents in the index are represented as a Map<String,String>.
 * This implementation does not provide a way uniquely identify a document, all deletions a updates are done following a "byQuery" approach.
 */
public class SimpleSearchIndex implements Closeable {

  private static final int MAX_RESULTS = 1_000_000;

  private static final Logger LOG = LoggerFactory.getLogger(SimpleSearchIndex.class);

  private final Directory mMapDirectory;

  private final Analyzer analyzer;

  private final IndexWriter indexWriter;

  private DirectoryReader indexReader;

  /**
   * @param dirPath where the index files are stored
   * @throws IOException in case of low-level exception
   */
  private SimpleSearchIndex(Path dirPath) throws IOException {
    mMapDirectory = new MMapDirectory(dirPath);
    //Builds an analyzer with the default stop words
    analyzer = new StandardAnalyzer();

    // IndexWriter Configuration
    IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

    //IndexWriter writes new index files to the directory
    indexWriter = new IndexWriter(mMapDirectory, iwc);
  }

  /**Creates a search index in the specified path*/
  public static SimpleSearchIndex create(String dirPath) throws IOException {
    return new SimpleSearchIndex(Paths.get(dirPath));
  }

  /**Creates a search index in the specified path*/
  public static SimpleSearchIndex create(Path dirPath) throws IOException {
    return new SimpleSearchIndex(dirPath);
  }

  /**
   * @return a IndexReader, it reuses the same instance of no changes are detected, otherwise ir creates a new reader
   * @throws IOException in case of error reading files
   */
  private IndexReader getIndexReader() throws IOException {
    if(Objects.isNull(indexReader)) { //first instance
      indexReader = DirectoryReader.open(indexWriter);
    } else { //Checked if changes are made
      DirectoryReader newDirectoryReader = DirectoryReader.openIfChanged(indexReader);
      indexReader = Objects.isNull(newDirectoryReader)? indexReader :newDirectoryReader;
    }
    return indexReader;
  }

  /** Gets a searcher using a index reader aware of changes*/
  private IndexSearcher getIndexSearcher() throws IOException {
    //Create index reader and searcher
    IndexSearcher indexSearcher = new IndexSearcher(getIndexReader());
    indexSearcher.setQueryCachingPolicy(new QueryCachingPolicy() {
      @Override
      public void onUse(Query query) {
        // do nothing
      }

      @Override
      public boolean shouldCache(Query query) throws IOException {
        return false;
      }
    });
    return indexSearcher;
  }

  /** Adds a document to the  index. Duplication of content is not checked bu this index.*/
  public long index(Document document) throws IOException {
    return indexWriter.addDocument(document);
  }

  /** Gets a document by its Lucene docId. This method must be used for search results only since ids can change after each commit.*/
  private Document getDocument(int docId) throws IOException {
    return getIndexSearcher().doc(docId);
  }

  /**
   * Deletes documents by using a field and a exact match against that field.
   * @param field to be used to find documents
   * @param value to be used as exact match
   * @return number of deleted documents
   * @throws IOException in case of low-level IO error
   */
  public long delete(String field, String value) throws IOException {
    LOG.info("Deleting from index field: {} with value: {}", field, value);
    long deleted = indexWriter.deleteDocuments(new Term(field, value));
    indexWriter.commit();
    return deleted;
  }

  /**
   * Updates documents by using a field and a exact match against that field.
   * @param field to be used to find documents
   * @param value to be used as exact match
   * @param fields fields to be updated
   * @return number of updated documents
   * @throws IOException
   */
  public long update(String field, String value, List<IndexableField> fields) throws IOException {
    LOG.info("Updating index field: {} with value: {}", field, value);
    fields.forEach(f -> LOG.info("Field: {} and value: {}", f.name(), f.stringValue()));
    long updated = indexWriter.updateDocument(new Term(field, value), fields);
    indexWriter.commit();
    return updated;
  }

  /**Converts a Lucene {@link Document} into a Map<String,String>.*/
  private static Map<String,String> docToMap(Document document) {
    return document.getFields().stream().collect(Collectors.toMap(IndexableField::name, IndexableField::stringValue));
  }

  /** Transforms a fieldName and q pair into Lucene query.
   * @throws IllegalArgumentException in case there are errors parsing the query
   */
  private Query toQuery(String fieldName, String q) throws IllegalArgumentException {
    try {
      //Build query
      QueryParser qp = new QueryParser(fieldName, analyzer);
      return qp.parse(q);
    } catch (ParseException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  /**
   * Utility method to convert results into a search.
   */
  private SearchResult doSearch(Query query) throws IOException {
    TopScoreDocCollector collector = TopScoreDocCollector.create(MAX_RESULTS);

    getIndexSearcher().search(query, collector);

    TopDocs topDocs = collector.topDocs(0, MAX_RESULTS);

    if (Objects.nonNull(topDocs.scoreDocs) && topDocs.scoreDocs.length > 0) {
      List<Map<String,String>> results = new ArrayList<>();
      Arrays.stream(topDocs.scoreDocs)
          .forEach(
              scoreDoc -> {
                try {
                  Optional.ofNullable(docToMap(getDocument(scoreDoc.doc)))
                      .filter(m -> !m.isEmpty())
                      .ifPresent(results::add);
                } catch (IOException ex) {
                  throw new RuntimeException(ex);
                }
              });
      if (!results.isEmpty()) {
        return SearchResult.of(topDocs.scoreDocs.length, results);
      }
    }
    return  SearchResult.empty();
  }

  /**
   * Performs a exact term search on a field value.
   * @param fieldName to be used for the search
   * @return a {@link SearchResult}, a SearchResults.result empty if no results.
   * @throws IOException in case lof low-level errors.
   */
  public SearchResult termSearch(String fieldName, String term) throws IOException {
    return doSearch(new TermQuery(new Term(fieldName, term)));
  }

  /**
   * Performs a search on a field value.
   * @param fieldName to be used for the search
   * @param q term query
   * @return a {@link SearchResult}, a SearchResults.result empty if no results.
   * @throws IOException in case lof low-level errors.
   */
  public SearchResult search(String fieldName, String q) throws IOException {
    return doSearch(toQuery(fieldName,q));
  }

  /**
   * Performs a exact multi-term search on a field value.
   *
   * @param termQueries term queries to include in the search
   * @param searchQueries full text queries to include in the search
   * @return a {@link SearchResult}, a SearchResults.result empty if no results.
   * @throws IOException in case lof low-level errors.
   */
  public SearchResult multiTermSearch(
      Map<String, Set<String>> termQueries,
      Map<String, String> searchQueries)
      throws IOException {
    BooleanQuery.Builder queryBuilder =
        new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(termQueries.size() + searchQueries.size());

    termQueries.forEach(
        (k, v) -> {
          // OR query, one matching clause is enough
          BooleanQuery.Builder multiTermQueryBuilder =
              new BooleanQuery.Builder().setMinimumNumberShouldMatch(1);

          v.forEach(
              value ->
                  multiTermQueryBuilder.add(
                      new BooleanClause(
                          new TermQuery(new Term(k, value)), BooleanClause.Occur.SHOULD)));

          queryBuilder.add(multiTermQueryBuilder.build(), BooleanClause.Occur.SHOULD);
        });

    searchQueries.forEach(
      (k, v) ->
        queryBuilder.add(
          new BooleanClause(toQuery(k, v), BooleanClause.Occur.SHOULD)));

    return doSearch(queryBuilder.build());
  }

  /**Close all used resources*/
  @Override
  public void close() {
    Stream.of(indexWriter, indexReader, analyzer, mMapDirectory).forEach(closeable -> {
      if (Objects.nonNull(closeable)) {
        try {
          closeable.close();
        } catch (IOException ex) {
          LOG.error("Error closing search index", ex);
        }
      }
    });
  }

  /**
   * Wraps the search results.
   */
  static class SearchResult {

    private final long totalHits;

    private final List<Map<String,String>> results;

    private SearchResult(long totalHits, List<Map<String,String>> results) {
      this.totalHits = totalHits;
      this.results = results;
    }

    /** @return a empty result with empty list of results and 0 totalHits.*/
    static SearchResult empty() {
      return new SearchResult(0, new ArrayList<>());
    }

    /** Factory method.*/
    static SearchResult of(long totalHits, List<Map<String,String>> results) {
      return new SearchResult(totalHits, results);
    }

    /** @return total number of results */
    public long getTotalHits() {
      return totalHits;
    }

    /** @return results/documents found */
    public List<Map<String,String>> getResults() {
      return results;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)  {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SearchResult that = (SearchResult) o;
      return totalHits == that.totalHits && results.equals(that.results);
    }

    @Override
    public int hashCode() {
      return Objects.hash(totalHits, results);
    }
  }

}
