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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.http.HttpResponse;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("UnstableApiUsage")
public abstract class BaseParameterizedResponseHandlerTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(BaseParameterizedResponseHandlerTest.class);

  protected abstract ResponseHandler<HttpResponse, ?> getResponseHandler();

  public static Stream<Arguments> getTestData(String fileName) throws IOException {
    List<Arguments> argumentsList = new ArrayList<>();
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode root = objectMapper.readValue(Resources.getResource(fileName), JsonNode.class);

    for (JsonNode node : root) {
      argumentsList.add(
          Arguments.of(
              node.get("comment").textValue(),
              node.get("file_name").textValue(),
              node.get("record_count") != null ? node.get("record_count").asInt() : null,
              node.get("hash") != null ? node.get("hash").asLong() : null,
              node.get("end_of_records") != null ? node.get("end_of_records").asBoolean() : null,
              node.get("exception_expected") != null && node.get("exception_expected").asBoolean()));
    }

    return argumentsList.stream();
  }

  @SuppressWarnings("Guava")
  public void testHandlingResponses(
      String fileName,
      Integer recordCount,
      Long contentHash,
      Boolean endOfRecords,
      boolean exceptionExpected) throws Exception {
    LOG.info(
        "Testing [{}]. Expecting [{}] records, [{}] hash, [{}] end of records",
        fileName, recordCount, contentHash, endOfRecords);

    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);

    URL resource = Resources.getResource(fileName);
    when(response.getEntity().getContent()).thenReturn(resource.openStream());

    ResponseHandler<HttpResponse, ?> handler = getResponseHandler();

    if (exceptionExpected) {
      // FatalCrawlException or ProtocolException
      assertThrows(Exception.class, () -> handler.handleResponse(response));
      assertFalse(handler.isValidState());
    } else {
      handler.handleResponse(response);
      assertTrue(handler.isValidState());
      assertEquals(contentHash, handler.getContentHash().orElse(null));
      assertEquals(recordCount, handler.getRecordCount().orElse(null));
      assertEquals(endOfRecords, handler.isEndOfRecords().orElse(null));
    }
  }
}
