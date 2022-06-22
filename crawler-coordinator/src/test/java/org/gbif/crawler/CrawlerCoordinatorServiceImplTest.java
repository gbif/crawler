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
package org.gbif.crawler;

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.metasync.api.MetadataSynchronizer;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.gbif.api.vocabulary.TagName.CONCEPTUAL_SCHEMA;
import static org.gbif.api.vocabulary.TagName.CRAWL_ATTEMPT;
import static org.gbif.api.vocabulary.TagName.DECLARED_COUNT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CrawlerCoordinatorServiceImplTest {

  @Mock private DatasetService datasetService;
  @Mock private MetadataSynchronizer metadataSynchronizer;
  private CuratorFramework curator;
  private CrawlerCoordinatorServiceImpl service;
  private UUID uuid = UUID.randomUUID();
  private Dataset dataset = new Dataset();
  private TestingServer server;

  @BeforeEach
  public void setup() throws Exception {
    server = new TestingServer();
    curator =
        CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .namespace("crawler")
            .retryPolicy(new RetryOneTime(1))
            .build();
    curator.start();
    ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), "/crawler/crawls");

    service = new CrawlerCoordinatorServiceImpl(curator, datasetService, metadataSynchronizer);
    dataset.setType(DatasetType.OCCURRENCE);
  }

  @AfterEach
  public void tearDown() throws IOException {
    server.stop();
    curator.close();
  }

  @Test
  public void testPrioritySort() {
    List<Endpoint> endpoints = Lists.newArrayList();

    Endpoint endpoint1 = new Endpoint();
    endpoint1.setType(EndpointType.DWC_ARCHIVE);
    endpoints.add(endpoint1);

    Endpoint endpoint2 = new Endpoint();
    endpoint2.setType(EndpointType.FEED);
    endpoints.add(endpoint2);

    Endpoint endpoint3 = new Endpoint();
    endpoint3.setType(EndpointType.DIGIR);
    endpoints.add(endpoint3);

    Endpoint endpoint4 = new Endpoint();
    endpoint4.setType(EndpointType.BIOCASE);
    endpoints.add(endpoint4);

    Endpoint endpoint5 = new Endpoint();
    endpoint5.setType(EndpointType.EML);
    endpoints.add(endpoint5);

    Endpoint endpoint6 = new Endpoint();
    endpoint6.setType(EndpointType.BIOCASE_XML_ARCHIVE);
    endpoints.add(endpoint6);

    List<Endpoint> sortedEndpoints = service.prioritySortEndpoints(endpoints);

    assertEquals(5, sortedEndpoints.size());
    assertThat(sortedEndpoints, contains(endpoint1, endpoint6, endpoint4, endpoint3, endpoint5));
    assertTrue(
        sortedEndpoints.indexOf(endpoint1) < sortedEndpoints.indexOf(endpoint5),
        "Priority of DwC-A is higher than its EML");
  }

  @Test
  public void testPrioritySortMultipleDwca() {
    List<Endpoint> endpoints = Lists.newArrayList();

    Endpoint endpoint1 = new Endpoint();
    endpoint1.setType(EndpointType.DWC_ARCHIVE);
    endpoints.add(endpoint1);

    Endpoint endpoint2 = new Endpoint();
    endpoint2.setType(EndpointType.DWC_ARCHIVE);
    endpoints.add(endpoint2);

    List<Endpoint> sortedEndpoints = service.prioritySortEndpoints(endpoints);

    assertEquals(2, sortedEndpoints.size());
    assertThat(sortedEndpoints, contains(endpoint1, endpoint2));
    assertEquals(endpoint1, sortedEndpoints.get(0));

    // now add created dates
    long now = System.currentTimeMillis();
    endpoint1.setCreated(new Date(now));
    endpoint2.setCreated(new Date(now - 100000));

    sortedEndpoints = service.prioritySortEndpoints(endpoints);
    assertEquals(2, sortedEndpoints.size());
    assertThat(sortedEndpoints, contains(endpoint2, endpoint1));
    assertEquals(endpoint2, sortedEndpoints.get(0));

    // now add created dates
    endpoint1.setCreated(new Date(now - 200000));
    sortedEndpoints = service.prioritySortEndpoints(endpoints);
    assertThat(sortedEndpoints, contains(endpoint1, endpoint2));
    assertEquals(endpoint1, sortedEndpoints.get(0));
  }

  @Test
  public void testValidation1() {
    when(datasetService.get(uuid)).thenReturn(dataset);
    when(datasetService.get(uuid)).thenReturn(null);
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> service.initiateCrawl(uuid, 5, Platform.ALL));
    assertTrue(exception.getMessage().contains("does not exist"));
  }

  @Test
  public void testValidation2() throws Exception {
    when(datasetService.get(uuid)).thenReturn(dataset);
    curator.create().forPath("/crawls/" + uuid.toString());
    AlreadyCrawlingException exception =
        assertThrows(AlreadyCrawlingException.class, () -> service.initiateCrawl(uuid, 5, Platform.ALL));
    assertTrue(exception.getMessage().contains("already scheduled"));
  }

  @Test
  public void testValidation3() {
    when(datasetService.get(uuid)).thenReturn(dataset);
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> service.initiateCrawl(uuid, 5, Platform.ALL));
    assertTrue(exception.getMessage().contains("endpoints"));
  }

  @Test
  public void testValidation4() {
    when(datasetService.get(uuid)).thenReturn(dataset);
    Endpoint endpoint = new Endpoint();
    endpoint.setType(EndpointType.TAPIR);
    dataset.getEndpoints().add(endpoint);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> service.initiateCrawl(uuid, 5, Platform.ALL));
    assertTrue(exception.getMessage().contains("conceptualSchema"));
  }

  @Test
  public void testValidation5() {
    when(datasetService.get(uuid)).thenReturn(dataset);
    Endpoint endpoint = new Endpoint();
    endpoint.setType(EndpointType.TAPIR);
    dataset.getEndpoints().add(endpoint);
    dataset.setDeleted(new Date());

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> service.initiateCrawl(uuid, 5, Platform.ALL));
    assertTrue(exception.getMessage().contains("deleted"));
  }

  @Test
  public void testSchedule() throws Exception {
    when(metadataSynchronizer.getDatasetCount(any(), any())).thenReturn(null);
    when(datasetService.get(uuid)).thenReturn(dataset);
    URI url = URI.create("http://gbif.org/index.html");
    Endpoint endpoint = new Endpoint();
    endpoint.setType(EndpointType.TAPIR);
    endpoint.setUrl(url);

    dataset.getEndpoints().add(endpoint);
    dataset.getMachineTags().add(MachineTag.newInstance(CONCEPTUAL_SCHEMA, "foo"));
    MachineTag tag = MachineTag.newInstance(CRAWL_ATTEMPT, "10");
    tag.setKey(123);
    dataset.getMachineTags().add(tag);
    dataset.getMachineTags().add(MachineTag.newInstance(DECLARED_COUNT, "1234"));
    service.initiateCrawl(uuid, 5, Platform.ALL);

    List<String> children = curator.getChildren().forPath("/crawls");
    assertEquals(1, children.size());
    assertEquals(uuid.toString(), children.get(0));

    byte[] bytes = curator.getData().forPath("/crawls/" + uuid);
    ObjectMapper mapper = new ObjectMapper();
    Map<?, ?> map = mapper.readValue(bytes, Map.class);

    Integer attempt = (Integer) map.get("attempt");
    assertEquals(11, attempt);

    bytes = curator.getData().forPath("/crawls/" + uuid + "/" + CrawlerNodePaths.DECLARED_COUNT);
    assertEquals("1234", new String(bytes, StandardCharsets.UTF_8));
  }
}
