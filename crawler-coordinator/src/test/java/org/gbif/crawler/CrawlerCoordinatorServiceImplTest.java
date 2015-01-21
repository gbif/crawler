package org.gbif.crawler;

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.crawler.constants.CrawlerNodePaths;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.gbif.api.vocabulary.TagName.CRAWL_ATTEMPT;
import static org.gbif.api.vocabulary.TagName.DECLARED_RECORD_COUNT;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CrawlerCoordinatorServiceImplTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  @Mock
  private DatasetService datasetService;
  private CuratorFramework curator;
  private CrawlerCoordinatorServiceImpl service;
  private UUID uuid = UUID.randomUUID();
  private Dataset dataset = new Dataset();
  private TestingServer server;

  @Before
  public void setup() throws Exception {
    server = new TestingServer();
    curator = CuratorFrameworkFactory.builder()
      .connectString(server.getConnectString())
      .namespace("crawler")
      .retryPolicy(new RetryOneTime(1))
      .build();
    curator.start();
    ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), "/crawler/crawls");

    service = new CrawlerCoordinatorServiceImpl(curator, datasetService);
    dataset.setType(DatasetType.OCCURRENCE);
    when(datasetService.get(uuid)).thenReturn(dataset);
  }

  @After
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

    List<Endpoint> sortedEndpoints = service.prioritySortEndpoints(endpoints);

    assertThat(sortedEndpoints.size(), equalTo(3));
    assertThat(sortedEndpoints, contains(endpoint1, endpoint4, endpoint3));
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

    assertThat(sortedEndpoints.size(), equalTo(2));
    assertThat(sortedEndpoints, contains(endpoint1, endpoint2));
    assertThat(sortedEndpoints.get(0), equalTo(endpoint1));

    // now add created dates
    long now = System.currentTimeMillis();
    endpoint1.setCreated(new Date(now));
    endpoint2.setCreated(new Date(now-100000));

    sortedEndpoints = service.prioritySortEndpoints(endpoints);
    assertThat(sortedEndpoints.size(), equalTo(2));
    assertThat(sortedEndpoints, contains(endpoint2, endpoint1));
    assertThat(sortedEndpoints.get(0), equalTo(endpoint2));

    // now add created dates
    endpoint1.setCreated(new Date(now-200000));
    sortedEndpoints = service.prioritySortEndpoints(endpoints);
    assertThat(sortedEndpoints, contains(endpoint1, endpoint2));
    assertThat(sortedEndpoints.get(0), equalTo(endpoint1));
  }

  @Test
  public void testValidation1() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("does not exist");

    when(datasetService.get(uuid)).thenReturn(null);
    service.initiateCrawl(uuid, 5);
  }

  @Test
  public void testValidation2() throws Exception {
    thrown.expect(AlreadyCrawlingException.class);
    thrown.expectMessage("already scheduled");

    curator.create().forPath("/crawls/" + uuid.toString());
    service.initiateCrawl(uuid, 5);
  }

  @Test
  public void testValidation3() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("endpoints");

    service.initiateCrawl(uuid, 5);
  }

  @Test
  public void testValidation4() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("conceptualSchema");

    Endpoint endpoint = new Endpoint();
    endpoint.setType(EndpointType.TAPIR);
    dataset.getEndpoints().add(endpoint);

    service.initiateCrawl(uuid, 5);
  }

  @Test
  public void testValidation5() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("deleted");

    Endpoint endpoint = new Endpoint();
    endpoint.setType(EndpointType.TAPIR);
    dataset.getEndpoints().add(endpoint);
    dataset.setDeleted(new Date());

    service.initiateCrawl(uuid, 5);
  }

  @Test
  public void testSchedule() throws Exception {
    URI url = URI.create("http://gbif.org/index.html");
    Endpoint endpoint = new Endpoint();
    endpoint.setType(EndpointType.TAPIR);
    endpoint.setUrl(url);

    dataset.getEndpoints().add(endpoint);
    dataset.getMachineTags().add(MachineTag.newInstance("metasync.gbif.org", "conceptualSchema", "foo"));
    MachineTag tag = MachineTag.newInstance(CRAWL_ATTEMPT.getNamespace().getNamespace(), CRAWL_ATTEMPT.getName(), "10");
    tag.setKey(123);
    dataset.getMachineTags().add(tag);
    dataset.getMachineTags()
      .add(MachineTag.newInstance(DECLARED_RECORD_COUNT.getNamespace().getNamespace(),
                                  DECLARED_RECORD_COUNT.getName(),
                                  "1234"));
    service.initiateCrawl(uuid, 5);

    List<String> children = curator.getChildren().forPath("/crawls");
    assertThat(children.size(), equalTo(1));
    assertThat(children.get(0), equalTo(uuid.toString()));

    byte[] bytes = curator.getData().forPath("/crawls/" + uuid);
    ObjectMapper mapper = new ObjectMapper();
    Map<?, ?> map = mapper.readValue(bytes, Map.class);

    Integer attempt = (Integer) map.get("attempt");
    assertThat(attempt, equalTo(11));

    bytes = curator.getData().forPath("/crawls/" + uuid + "/" + CrawlerNodePaths.DECLARED_COUNT);
    assertThat("1234", equalTo(new String(bytes)));
  }
}
