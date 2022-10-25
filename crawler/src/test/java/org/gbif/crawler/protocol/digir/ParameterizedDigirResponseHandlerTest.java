/*
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
package org.gbif.crawler.protocol.digir;

import org.gbif.crawler.ResponseHandler;
import org.gbif.crawler.protocol.BaseParameterizedResponseHandlerTest;

import java.util.stream.Stream;

import org.apache.http.HttpResponse;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ParameterizedDigirResponseHandlerTest extends BaseParameterizedResponseHandlerTest {

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("data")
  public void testHandlingResponsesDigir(
      String comment,
      String fileName,
      Integer recordCount,
      Long contentHash,
      Boolean endOfRecords,
      boolean exceptionExpected) throws Exception {
    super.testHandlingResponses(fileName, recordCount, contentHash, endOfRecords, exceptionExpected);
  }

  @Override
  protected ResponseHandler<HttpResponse, ?> getResponseHandler() {
    return new DigirResponseHandler();
  }

  public static Stream<Arguments> data() throws Exception {
    return getTestData("org/gbif/crawler/protocol/digir/responses.json");
  }
}
