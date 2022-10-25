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
package org.gbif.crawler.emlpusher;

import java.io.File;

import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;

public class PushEmlConfiguration {

  @Parameter(names = "--registry-ws-url")
  @NotNull
  public String registryWsUrl = "http://api.gbif.org/v1/";

  @Parameter(names = "--registry-user")
  @NotNull
  public String registryUser = "eml-pusher@gbif.org";

  @Parameter(names = "--registry-password")
  @NotNull
  public String registryPassword;

  @Parameter(names = "--unpacked-repository")
  @NotNull
  public File unpackedRepository;
}
