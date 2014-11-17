package org.gbif.crawler.xml.fragmenter;

import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.CrawlResponseMessage;
import org.gbif.common.messaging.api.messages.OccurrenceFragmentedMessage;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.ws.client.CrawlerWsClient;
import org.gbif.occurrence.OccurrenceParser;
import org.gbif.occurrence.ParsingException;
import org.gbif.occurrence.parsing.RawXmlOccurrence;
import org.gbif.utils.HttpUtil;
import org.gbif.ws.json.JacksonJsonContextResolver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.AbstractIdleService;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.client.apache4.ApacheHttpClient4Handler;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.gbif.crawler.constants.CrawlerNodePaths.CRAWL_INFO;
import static org.gbif.crawler.constants.CrawlerNodePaths.FRAGMENTS_EMITTED;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_FRAGMENTED_ERROR;
import static org.gbif.crawler.constants.CrawlerNodePaths.PAGES_FRAGMENTED_SUCCESSFUL;

public class FragmenterService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(FragmenterService.class);

  private final FragmenterConfiguration configuration;

  private MessagePublisher publisher;
  private MessageListener listener;

  private CuratorFramework curator;

  private CrawlerWsClient crawlerWsClient;

  public FragmenterService(FragmenterConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {
    LoadingCache<String, DistributedAtomicLong> cache =
      CacheBuilder.newBuilder().maximumSize(1000).build(new CacheLoader<String, DistributedAtomicLong>() {

        private final RetryPolicy retryPolicy = new RetryNTimes(5, 1000);

        @Override
        public DistributedAtomicLong load(String key) throws Exception {
          return new DistributedAtomicLong(curator, CrawlerNodePaths.buildPath(CRAWL_INFO, key), retryPolicy);
        }
      });

    curator = configuration.zooKeeper.getCuratorFramework();

    // http connection setup for the crawler-ws-client
    ApacheHttpClient4Handler hch =
      new ApacheHttpClient4Handler(HttpUtil.newMultithreadedClient(1000, 100, 10), null, false);
    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getClasses().add(JacksonJsonContextResolver.class);
    clientConfig.getClasses().add(JacksonJsonProvider.class);
    clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    Client client = new ApacheHttpClient4(hch, clientConfig);
    crawlerWsClient = new CrawlerWsClient(client.resource(configuration.crawlerWsUrl));

    publisher = new DefaultMessagePublisher(configuration.messaging.getConnectionParameters());
    listener = new MessageListener(configuration.messaging.getConnectionParameters());
    listener.listen(configuration.queueName, configuration.poolSize,
      new CrawlResponseMessageMessageCallback(publisher, cache, crawlerWsClient));
  }

  @Override
  protected void shutDown() throws Exception {
    publisher.close();
    listener.close();
    curator.close();
  }


  private static class CrawlResponseMessageMessageCallback extends AbstractMessageCallback<CrawlResponseMessage> {

    private final MessagePublisher publisher;

    private final LoadingCache<String, DistributedAtomicLong> counterCache;

    private final CrawlerWsClient crawlerWsClient;

    private CrawlResponseMessageMessageCallback(MessagePublisher publisher,
      LoadingCache<String, DistributedAtomicLong> counterCache, CrawlerWsClient crawlerWsClient) {
      this.publisher = publisher;
      this.counterCache = counterCache;
      this.crawlerWsClient = crawlerWsClient;
    }

    @Override
    public void handleMessage(CrawlResponseMessage message) {
      MDC.put("datasetKey", message.getDatasetUuid().toString());
      LOG.debug("Received crawl response message for [{}]", message.getDatasetUuid());
      Stopwatch stopwatch = new Stopwatch().start();
      OccurrenceParser parser = new OccurrenceParser();
      List<RawXmlOccurrence> list = null;
      try {
        list = parser.parseStream(new ByteArrayInputStream(message.getResponse()));
      } catch (ParsingException e) {
        LOG.warn("Error while parsing response for dataset [{}], status [{}]", message.getDatasetUuid(),
          message.getStatus(), e);
        incrementCounter(message.getDatasetUuid() + "/" + PAGES_FRAGMENTED_ERROR, 1);
      }

      if (list == null || list.isEmpty()) {
        incrementCounter(message.getDatasetUuid() + "/" + PAGES_FRAGMENTED_SUCCESSFUL, 1);
        return;
      }

      LOG.info("Sending [{}] occurrences for dataset [{}]", list.size(), message.getDatasetUuid());

      DatasetProcessStatus status = crawlerWsClient.getDatasetProcessStatus(message.getDatasetUuid());
      EndpointType endpointType = null;
      if (status != null && status.getCrawlJob() != null) {
        endpointType = status.getCrawlJob().getEndpointType();
      }

      if (endpointType == null) {
        LOG.warn("EndpointType is null - this shouldn't happen. Can't send fragment messages for " + "dataset [{}]",
          message.getDatasetUuid());
        incrementCounter(message.getDatasetUuid() + "/" + PAGES_FRAGMENTED_ERROR, 1);
      } else {
        for (RawXmlOccurrence rawXmlOccurrence : list) {
          byte[] bytes = rawXmlOccurrence.getXml().getBytes(Charsets.UTF_8);
          OccurrenceFragmentedMessage resultMessage =
            new OccurrenceFragmentedMessage(message.getDatasetUuid(), message.getAttempt(), bytes,
              rawXmlOccurrence.getSchemaType(), endpointType, null);

          try {
            publisher.send(resultMessage);
          } catch (IOException e) {
            LOG.error("Error sending message", e);
          }
        }
        incrementCounter(message.getDatasetUuid() + "/" + PAGES_FRAGMENTED_SUCCESSFUL, 1);
        incrementCounter(message.getDatasetUuid() + "/" + FRAGMENTS_EMITTED, list.size());
      }

      stopwatch.stop();
      LOG.debug("Finished processing crawl response message for [{}] in [{}ms]", message.getDatasetUuid(),
        stopwatch.elapsed(TimeUnit.MILLISECONDS));
      MDC.remove("datasetKey");
    }

    private void incrementCounter(String path, long count) {
      DistributedAtomicLong counter = counterCache.getUnchecked(path);
      try {
        AtomicValue<Long> value = counter.add(count);
        if (!value.succeeded()) {
          LOG.error("Failed to update counter [{}]", path);
        }
      } catch (Exception e) {
        LOG.error("Failed to update counter [{}]", path, e);
      }
    }

  }

}
