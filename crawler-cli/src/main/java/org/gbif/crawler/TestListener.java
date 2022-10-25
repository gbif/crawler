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

import org.gbif.api.model.registry.Dataset;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessageRegistry;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.messages.RegistryChangeMessage;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This is a class that is very useful for testing during crawl. It will likely be deleted, or moved
 * to a new location in the near future.
 */
public class TestListener {

  private static final Logger LOG = LoggerFactory.getLogger(TestListener.class);

  public static void main(String[] args) throws IOException {
    ConnectionParameters connectionParameters =
        new ConnectionParameters("localhost", 5672, "guest", "guest", "/");
    ObjectMapper objectMapper =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    MessageListener listener =
        new MessageListener(connectionParameters, new DefaultMessageRegistry(), objectMapper);

    listener.listen(
        "testlistener",
        "registry.change.#",
        100,
        new AbstractMessageCallback<RegistryChangeMessage>() {

          @Override
          public void handleMessage(RegistryChangeMessage message) {
            switch (message.getChangeType()) {
              case CREATED:
                LOG.info(
                    "Message type: [{}], class [{}]",
                    message.getChangeType(),
                    message.getObjectClass());
                if (message.getObjectClass().equals(Dataset.class)) {
                  Dataset dataset = (Dataset) message.getNewObject();
                  LOG.info(
                      "Created new Dataset with Key [{}] and Title [{}]",
                      dataset.getKey(),
                      dataset.getTitle());
                }
                break;
              case UPDATED:
                if (message.getObjectClass().equals(Dataset.class)) {
                  Dataset newDataset = (Dataset) message.getNewObject();
                  Dataset oldDataset = (Dataset) message.getOldObject();
                  LOG.info(
                      "Updated dataset with key [{}], old Title [{}], new Title [{}]",
                      newDataset.getKey(),
                      oldDataset.getTitle(),
                      newDataset.getTitle());
                }
                break;
              case DELETED:
                if (message.getObjectClass().equals(Dataset.class)) {
                  Dataset dataset = (Dataset) message.getOldObject();
                  LOG.info(
                      "Deleted Dataset with Key [{}] and Title [{}]",
                      dataset.getKey(),
                      dataset.getTitle());
                }
                break;
            }
          }
        });
  }
}
