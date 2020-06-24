/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.protocol;

import org.gbif.crawler.ResponseHandler;
import org.gbif.crawler.exception.FatalCrawlException;
import org.gbif.crawler.exception.ProtocolException;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.http.HttpResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.io.Resources;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BaseParameterizedResponseHandlerTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(BaseParameterizedResponseHandlerTest.class);

  /**
   * This loads test data from JSON files and returns them in a way suitable for JUnit's {@link
   * org.junit.runners.Parameterized} runner.
   *
   * <p>All JSON files must consist of a top level array of objects with the following fields:
   *
   * <ul>
   *   <li>file_name (string)
   *   <li>record_count (number), optional
   *   <li>hash (number), optional
   *   <li>end_of_records (boolean), optional
   *   <li>exception_expected (boolean), optional
   * </ul>
   *
   * @param fileName to read the test data from
   * @return a collection of Object arrays containing the test data
   */
  public static Collection<Object[]> getTestData(String fileName) throws IOException {
    Collection<Object[]> objects = new ArrayList<Object[]>();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readValue(Resources.getResource(fileName), JsonNode.class);

    for (JsonNode node : root) {
      objects.add(
          new Object[] {
            node.get("file_name").textValue(),
            node.get("record_count") == null
                ? Optional.absent()
                : Optional.of(node.get("record_count").asInt()),
            node.get("hash") == null ? Optional.absent() : Optional.of(node.get("hash").asLong()),
            node.get("end_of_records") == null
                ? Optional.absent()
                : Optional.of(node.get("end_of_records").asBoolean()),
            node.path("exception_expected").asBoolean(false)
          });
    }

    return objects;
  }

  public BaseParameterizedResponseHandlerTest(
      String fileName,
      Optional<Integer> recordCount,
      Optional<Long> contentHash,
      Optional<Boolean> endOfRecords,
      boolean exceptionExpected) {
    this.fileName = fileName;
    this.recordCount = recordCount;
    this.contentHash = contentHash;
    this.endOfRecords = endOfRecords;
    this.exceptionExpected = exceptionExpected;
  }

  private final String fileName;
  private final Optional<Integer> recordCount;
  private final Optional<Long> contentHash;
  private final Optional<Boolean> endOfRecords;
  private final boolean exceptionExpected;

  protected abstract ResponseHandler<HttpResponse, ?> getResponseHandler();

  @Test
  public void testHandlingResponses() throws Exception {
    LOG.info(
        "Testing [{}]. Expecting [{}] records, [{}] hash, [{}] end of records",
        new Object[] {fileName, recordCount, contentHash, endOfRecords});

    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);

    URL resource = Resources.getResource(fileName);
    when(response.getEntity().getContent()).thenReturn(resource.openStream());

    ResponseHandler<HttpResponse, ?> handler = getResponseHandler();

    try {
      handler.handleResponse(response);
      if (exceptionExpected) {
        assertThat(handler.isValidState()).isFalse();
        fail();
      }
    } catch (ProtocolException e) {
      if (exceptionExpected) {
        assertThat(handler.isValidState()).isFalse();
        return;
      }

      throw e;
    } catch (FatalCrawlException e) {
      if (exceptionExpected) {
        assertThat(handler.isValidState()).isFalse();
        return;
      }

      throw e;
    }

    assertThat(handler.isValidState()).as("Valid state").isTrue();
    assertThat(handler.getContentHash()).as("Content hash").isEqualTo(contentHash);
    assertThat(handler.getRecordCount()).as("Record count").isEqualTo(recordCount);
    assertThat(handler.isEndOfRecords()).as("End of records").isEqualTo(endOfRecords);
  }
}
