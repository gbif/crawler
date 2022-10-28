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
package org.gbif.crawler;

import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.common.messaging.api.messages.StartCrawlMessage;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.same;
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

    verify(service)
        .initiateCrawl(message.getDatasetUuid(), message.getPriority(), message.getPlatform());
  }

  @Test
  public void testFailedCallback() {
    CrawlerCoordinatorService service = mock(CrawlerCoordinatorService.class);
    doThrow(new IllegalArgumentException("foo"))
        .when(service)
        .initiateCrawl(isA(UUID.class), eq(5), same(Platform.ALL));

    StartCrawlMessageCallback callback = new StartCrawlMessageCallback(service);

    StartCrawlMessage message = new StartCrawlMessage(UUID.randomUUID(), 5);
    callback.handleMessage(message);
  }
}
