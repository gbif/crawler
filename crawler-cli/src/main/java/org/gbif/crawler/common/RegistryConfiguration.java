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

import com.beust.jcommander.Parameter;
import java.time.Duration;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.cli.PropertyName;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

/**
 * A configuration class which can be used to get all the details needed to create a writable
 * connection to the GBIF registry.
 */
@ToString
public class RegistryConfiguration {

  @Parameter(names = "--registry-ws")
  @PropertyName("registry.ws.url")
  @NotNull
  public String wsUrl = "http://api.gbif.org/v1/";

  @Parameter(names = "--registry-user")
  @NotNull
  public String user;

  @Parameter(names = "--registry-password", password = true)
  @NotNull
  public String password;

  /**
   * Convenience method to provide a ws client factory. The factory will be used to create writable
   * registry clients.
   *
   * @return writable client factory
   */
  public ClientBuilder newClientBuilder() {
    // setup writable registry client
    return new ClientBuilder()
        .withUrl(wsUrl)
        .withCredentials(user, password)
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
        // This will give up to 40 tries, from 2 to 75 seconds apart, over at most 13 minutes (approx)
        .withExponentialBackoffRetry(Duration.ofSeconds(2), 1.1, 40);
  }
}
