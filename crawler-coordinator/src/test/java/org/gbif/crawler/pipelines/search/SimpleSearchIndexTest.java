package org.gbif.crawler.pipelines.search;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import com.google.common.io.Files;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests cases for class {@link SimpleSearchIndex}. */
public class SimpleSearchIndexTest {

  // instance initialized after each test
  private static SimpleSearchIndex simpleSearchIndex;

  @Before
  public void init() throws IOException {
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    simpleSearchIndex = SimpleSearchIndex.create(tmpDir.toPath());
  }

  @After
  public void tearDown() {
    if (Objects.nonNull(simpleSearchIndex)) {
      simpleSearchIndex.close();
    }
  }

  /** Performs simple indexing a paging test. */
  @Test
  public void indexingTest() throws IOException {
    // State
    simpleSearchIndex.index(createStringDocument("val", "hi"));
    simpleSearchIndex.index(createTextDocument("val", "hi 2"));
    simpleSearchIndex.index(createTextDocument("val", "hi 3"));

    // When perform a search of all results
    SimpleSearchIndex.SearchResult response = simpleSearchIndex.termSearch("val", "hi", 0, 5);

    // Expect
    assertEquals(3, response.getTotalHits());
    assertEquals(3, response.getResults().size());

    // When perform a search of last result
    response = simpleSearchIndex.termSearch("val", "hi", 0, 1);

    // Expect
    assertEquals(1, response.getTotalHits());
    assertEquals(1, response.getResults().size());

    // When perform a phrase search of all results
    response = simpleSearchIndex.search("val", "2", 0, 3);

    // Expect
    assertEquals(1, response.getTotalHits());
    assertEquals(1, response.getResults().size());
  }

  @Test
  public void searchTest() throws IOException {
    // State
    simpleSearchIndex.index(createTextDocument("val", "dataset one"));
    simpleSearchIndex.index(createTextDocument("val", "dataset two"));

    // When do a search query
    SimpleSearchIndex.SearchResult response = simpleSearchIndex.search("val", "data*", 0, 5);

    // Expect
    assertEquals(2, response.getTotalHits());
    assertEquals(2, response.getResults().size());

    // When do a search query
    response = simpleSearchIndex.search("val", "ata on*", 0, 5);

    // Expect
    assertEquals(1, response.getTotalHits());
    assertEquals(1, response.getResults().size());

    // When do a search query
    response = simpleSearchIndex.search("val", "two*", 0, 5);

    // Expect
    assertEquals(1, response.getTotalHits());
    assertEquals(1, response.getResults().size());
  }

  /** Test additions to the index. */
  @Test
  public void indexingAppendTest() throws IOException {
    // State
    simpleSearchIndex.index(createTextDocument("val", "hi 2"));
    simpleSearchIndex.index(createTextDocument("val", "hi 2"));

    // When
    SimpleSearchIndex.SearchResult response = simpleSearchIndex.search("val", "hi", 0, 2);

    // Expect
    assertEquals(2, response.getTotalHits());

    // New state
    simpleSearchIndex.index(createTextDocument("val", "hi 2"));

    // When state changed
    response = simpleSearchIndex.search("val", "hi", 0, 3);

    // Expect
    assertEquals(3, response.getTotalHits());
    assertEquals(3, response.getResults().size());
  }

  /** Tests the deletion of 1 document. */
  @Test
  public void deleteTest() throws IOException {
    // State
    simpleSearchIndex.index(createStringDocument("val", "hi"));

    // When deletes a document and try to retrieve it
    simpleSearchIndex.delete("val", "hi");
    SimpleSearchIndex.SearchResult response = simpleSearchIndex.termSearch("val", "hi", 0, 3);

    // Expect
    assertEquals(0, response.getTotalHits());
    assertEquals(0, response.getResults().size());
  }

  /** Tests the deletion of 1 document. */
  @Test
  public void updateTest() throws IOException {
    // State
    simpleSearchIndex.index(createStringDocument("val", "hi"));
    simpleSearchIndex.index(createStringDocument("val", "hi2"));
    simpleSearchIndex.index(createTextDocument("val", "hi 3"));

    // When update a document and search of rit
    simpleSearchIndex.update("val", "hi2", createStringDocument("val", "hi4").getFields());
    SimpleSearchIndex.SearchResult response = simpleSearchIndex.termSearch("val", "hi2", 0, 3);

    // Expect
    assertEquals(0, response.getTotalHits());

    // When search for the updated value
    response = simpleSearchIndex.termSearch("val", "hi4", 0, 3);

    // Expect
    assertEquals(1, response.getTotalHits());
    assertEquals(1, response.getResults().size());
  }

  private Document createTextDocument(String field, String value) {
    Document doc = new Document();
    doc.add(new TextField(field, value, Field.Store.YES));
    return doc;
  }

  private Document createStringDocument(String field, String value) {
    Document doc = new Document();
    doc.add(new StringField(field, value, Field.Store.YES));
    return doc;
  }
}
