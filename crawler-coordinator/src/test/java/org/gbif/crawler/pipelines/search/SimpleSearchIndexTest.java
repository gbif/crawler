package org.gbif.crawler.pipelines.search;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests cases for class {@link SimpleSearchIndex}.
 */
public class SimpleSearchIndexTest {


    //instance initialized after each test
    private static SimpleSearchIndex simpleSearchIndex;

    @Before
    public void init() throws IOException {
      simpleSearchIndex = SimpleSearchIndex.create(Files.createTempDir().toPath());
    }

    @After
    public void tearDown() {
      if (Objects.nonNull(simpleSearchIndex)) {
        simpleSearchIndex.close();
      }
    }

  /**
   * Performs simple indexing a paging test.
   */
  @Test
    public void indexingTest() throws IOException {
      //State
      simpleSearchIndex.index(Collections.singletonMap("val", "hi"));
      simpleSearchIndex.index(Collections.singletonMap("val", "hi2"));
      simpleSearchIndex.index(Collections.singletonMap("val", "hi3"));

      //When perform a search of 2 first results
      SimpleSearchIndex.SearchResult response = simpleSearchIndex.search("val", "hi*", 2);

      //Expect
      Assert.assertEquals(3, response.getTotalHits());
      Assert.assertEquals(2, response.getResults().size());

      //When perform a paging search of the second result in page size of 1
      response = simpleSearchIndex.search("val", "hi*", 2, 1);

      //Expect
      Assert.assertEquals(3, response.getTotalHits());
      Assert.assertEquals(1, response.getResults().size());
   }

  /**
   * Test additions to the index.
   */
  @Test
  public void indexingAppendTest() throws IOException {
    //State
    simpleSearchIndex.index(Collections.singletonMap("val", "hi"));
    simpleSearchIndex.index(Collections.singletonMap("val", "hi2"));

    //When
    SimpleSearchIndex.SearchResult response = simpleSearchIndex.search("val", "hi*", 1, 2);

    //Expect
    Assert.assertEquals(2, response.getTotalHits());

    //New state
    simpleSearchIndex.index(Collections.singletonMap("val", "hi3"));

    //When state changed
    response = simpleSearchIndex.search("val", "hi*", 1, 3);

    //Expect
    Assert.assertEquals(3, response.getTotalHits());
    Assert.assertEquals(3, response.getResults().size());
  }

  /**
   * Tests the deletion of 1 document.
   */
  @Test
  public void deleteTest() throws IOException {
    //State
    simpleSearchIndex.index(Collections.singletonMap("val", "hi"));
    simpleSearchIndex.index(Collections.singletonMap("val", "hi2"));
    simpleSearchIndex.index(Collections.singletonMap("val", "hi3"));

    //When deletes a document and try to retrieve it
    simpleSearchIndex.delete("val", "hi2");
    SimpleSearchIndex.SearchResult response = simpleSearchIndex.search("val", "hi*",3);


    //Expect
    Assert.assertEquals(2, response.getTotalHits());
    Assert.assertEquals(2, response.getResults().size());
  }

  /**
   * Tests the deletion of 1 document.
   */
  @Test
  public void updateTest() throws IOException {
    //State
    simpleSearchIndex.index(Collections.singletonMap("val", "hi"));
    simpleSearchIndex.index(Collections.singletonMap("val", "hi2"));
    simpleSearchIndex.index(Collections.singletonMap("val", "hi3"));

    //When update a document and search of rit
    simpleSearchIndex.update("val", "hi2", Collections.singletonMap("val", "hi4"));
    SimpleSearchIndex.SearchResult response = simpleSearchIndex.search("val", "hi2", 3);

    //Expect
    Assert.assertEquals(0, response.getTotalHits());

    //When search for the updated value
    response = simpleSearchIndex.search("val", "hi4", 3);

    //Expect
    Assert.assertEquals(1, response.getTotalHits());
    Assert.assertEquals(1, response.getResults().size());

  }

}
