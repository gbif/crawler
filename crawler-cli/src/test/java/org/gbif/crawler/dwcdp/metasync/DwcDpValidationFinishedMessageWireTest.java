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
package org.gbif.crawler.dwcdp.metasync;

import org.gbif.common.messaging.api.messages.DwcDpValidationFinishedMessage;
import org.gbif.common.messaging.util.MessageUtils;

import java.time.LocalDateTime;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The analyser sends the whole {@code AnalysisExecution} as {@code validationReport}, with
 * {@code LocalDateTime} values (dwc-dp-analyser-service 0.0.9). The crawler must read it with
 * postal's mapper settings.
 */
class DwcDpValidationFinishedMessageWireTest {

  private static final ObjectMapper MAPPER = MessageUtils.configure(new ObjectMapper());

  @Test
  void readsTheAnalysisExecutionReport() throws Exception {
    UUID datasetKey = UUID.randomUUID();
    String json =
        """
        {"datasetUuid": "%s", "attempt": 3, "valid": false,
         "validationReport": {
           "result": null,
           "metadata": {"started": "2026-11-18T10:00:00", "finished": "2026-11-18T10:00:05.123",
                        "features": [], "isValid": false},
           "version": "1.0"}}
        """
            .formatted(datasetKey);

    DwcDpValidationFinishedMessage message =
        MAPPER.readValue(json, DwcDpValidationFinishedMessage.class);

    assertEquals(datasetKey, message.getDatasetUuid());
    assertEquals(3, message.getAttempt());
    assertFalse(message.isValid());
    assertEquals(
        LocalDateTime.parse("2026-11-18T10:00:00"),
        message.getValidationReport().metadata().started());
    assertEquals(DwcDpValidationFinishedMessage.ROUTING_KEY, message.getRoutingKey());
  }
}
