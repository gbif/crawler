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

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Actuator on the same port as the API, e.g. if a deployment unsets the management port: only
 * health and info are reachable. src/test/resources/application.yml exposes every endpoint.
 */
@SpringBootTest
@AutoConfigureMockMvc
public class WebSecurityConfigTest {

  @Autowired private MockMvc mockMvc;

  @MockitoBean(name = "datasetProcessService")
  private DatasetProcessService datasetProcessService;

  @Test
  public void testHealthAndInfoArePublic() throws Exception {
    mockMvc.perform(get("/actuator/health")).andExpect(status().is2xxSuccessful());
    mockMvc.perform(get("/actuator/info")).andExpect(status().is2xxSuccessful());
  }

  @Test
  public void testOtherActuatorEndpointsAreDenied() throws Exception {
    mockMvc.perform(get("/actuator/env")).andExpect(status().isForbidden());
    mockMvc.perform(get("/actuator/loggers")).andExpect(status().isForbidden());
    mockMvc.perform(get("/actuator")).andExpect(status().isForbidden());
    mockMvc
        .perform(
            post("/actuator/loggers/ROOT")
                .contentType("application/json")
                .content("{\"configuredLevel\":\"OFF\"}"))
        .andExpect(status().isForbidden());
  }

  @Test
  public void testApiIsPublic() throws Exception {
    when(datasetProcessService.getRunningDatasetProcesses()).thenReturn(Set.of());
    mockMvc.perform(get("/dataset/process/running")).andExpect(status().isOk());
  }

  @Test
  public void testCorsAllowsOnlyReads() throws Exception {
    mockMvc
        .perform(
            options("/dataset/process/running")
                .header("Origin", "https://example.org")
                .header("Access-Control-Request-Method", "GET"))
        .andExpect(status().isOk());
    mockMvc
        .perform(
            options("/dataset/process/running")
                .header("Origin", "https://example.org")
                .header("Access-Control-Request-Method", "DELETE"))
        .andExpect(status().isForbidden());
  }
}
