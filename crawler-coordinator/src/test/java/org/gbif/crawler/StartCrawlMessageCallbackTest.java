package org.gbif.crawler;

import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.common.messaging.api.messages.StartCrawlMessage;

import java.util.UUID;

import org.junit.Test;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class StartCrawlMessageCallbackTest {

  @Test
  public void testCallback() {
    CrawlerCoordinatorService service = mock(CrawlerCoordinatorService.class);

    StartCrawlMessageCallback callback = new StartCrawlMessageCallback(service);

    StartCrawlMessage message = new StartCrawlMessage(UUID.randomUUID(), 5);
    callback.handleMessage(message);

    verify(service).initiateCrawl(message.getDatasetUuid(), message.getPriority().get(), message.getPlatform());
  }

  @Test
  public void testFailedCallback() {
    CrawlerCoordinatorService service = mock(CrawlerCoordinatorService.class);
    doThrow(new IllegalArgumentException("foo")).when(service).initiateCrawl(isA(UUID.class), eq(5), same(Platform.ALL));

    StartCrawlMessageCallback callback = new StartCrawlMessageCallback(service);

    StartCrawlMessage message = new StartCrawlMessage(UUID.randomUUID(), 5);
    callback.handleMessage(message);
  }
}
