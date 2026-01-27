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
package org.gbif.crawler.dwca;

import org.gbif.cli.IgnoreProperty;
import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.RegistryConfiguration;

import java.io.File;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.ToString;

@ToString
public class DwcaConfiguration {

  public static final String DWCA_SUFFIX = ".dwca";
  public static final String METADATA_FILE = "metadata.xml";

  @ParametersDelegate @Valid @NotNull @IgnoreProperty
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate @Valid @NotNull
  public RegistryConfiguration registry = new RegistryConfiguration();

  @Parameter(names = "--pool-size")
  @Min(1)
  public int poolSize = 10;

  @Parameter(names = "--archive-repository")
  @NotNull
  public File archiveRepository;

  @Parameter(names = "--unpacked-repository")
  @NotNull
  public File unpackedRepository;

}
