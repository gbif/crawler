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
package org.gbif.crawler.ws.config;

import org.gbif.api.service.crawler.DatasetProcessService;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Actuator on a separate management port: all endpoints there, none on the public port. */
@SpringBootTest(
    webEnvironment = WebEnvironment.RANDOM_PORT,
    properties = "management.server.port=0")
public class ManagementPortTest {

  @LocalServerPort private int serverPort;

  @LocalManagementPort private int managementPort;

  @Autowired private TestRestTemplate restTemplate;

  @MockitoBean(name = "datasetProcessService")
  private DatasetProcessService datasetProcessService;

  @Test
  public void testActuatorNotOnPublicPort() {
    assertEquals(404, status(serverPort, "/actuator/env"));
    assertEquals(404, status(serverPort, "/actuator/health"));
  }

  @Test
  public void testActuatorOnManagementPort() {
    assertEquals(200, status(managementPort, "/actuator/env"));
    assertEquals(200, status(managementPort, "/actuator/loggers"));
    assertEquals(200, status(managementPort, "/actuator/info"));
  }

  private int status(int port, String path) {
    return restTemplate
        .getForEntity("http://localhost:" + port + path, String.class)
        .getStatusCode()
        .value();
  }
}
