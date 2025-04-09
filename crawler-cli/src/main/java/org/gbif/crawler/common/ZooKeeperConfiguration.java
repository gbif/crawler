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
package org.gbif.crawler.common;

import java.io.IOException;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.ToString;

/**
 * A configuration class which can be used to get all the details needed to create a connection to
 * ZooKeeper needed by the Curator Framework. It provides a convenience method ({@link
 * #getCuratorFramework()} ()} to actually get a {@link CuratorFramework} object when populated
 * fully.
 */
@SuppressWarnings("PublicField")
@ToString
public class ZooKeeperConfiguration {

  @Parameter(
      names = "--zk-connection-string",
      description = "The connection string to connect to ZooKeeper")
  @NotNull
  public String connectionString;

  @Parameter(
      names = "--zk-namespace",
      description = "The namespace in ZooKeeper under which all data lives")
  @NotNull
  public String namespace;

  @Parameter(
      names = "--zk-sleep-time",
      description = "Initial amount of time to wait between retries in ms")
  @Min(1)
  public int baseSleepTime = 1000;

  @Parameter(names = "--zk-max-retries", description = "Max number of times to retry")
  @Min(1)
  public int maxRetries = 10;

  /**
   * This method returns a connection object to ZooKeeper with the provided settings and creates and
   * starts a {@link CuratorFramework}. These settings are not validated in this method so only call
   * it when the object has been validated.
   *
   * @return started CuratorFramework
   * @throws IOException if connection fails
   */
  @JsonIgnore
  public CuratorFramework getCuratorFramework() throws IOException {
    CuratorFramework curator =
        CuratorFrameworkFactory.builder()
            .namespace(namespace)
            .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries))
            .connectString(connectionString)
            .build();
    curator.start();
    return curator;
  }

}
