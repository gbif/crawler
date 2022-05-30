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
package org.gbif.crawler.metasync.api;

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Endpoint;

import java.util.List;
import java.util.UUID;

/**
 * This interface specifies all operations necessary to do a synchronization of an Installation from
 * our registry against the actual Endpoint.
 */
public interface MetadataSynchronizer {

  /**
   * Runs a synchronization against the installation provided.
   *
   * <p>All datasets from this installation will be updated, old ones deleted and new ones
   * registered if needed.
   *
   * @param key of the installation to synchronize
   * @return the result of the synchronization
   * @throws IllegalArgumentException if no technical installation with the given key exists, the
   *     installation doesn't have any endpoints or none that we can use to gather metadata (might
   *     also be missing the proper protocol handler)
   */
  SyncResult synchronizeInstallation(UUID key);

  /**
   * synchronizes all registered Installations ignoring any failures (because there will definitely
   * be failures due to unsupported Installation types and missing Endpoints). This will run single
   * threaded.
   */
  List<SyncResult> synchronizeAllInstallations();

  /**
   * synchronizes all registered Installations ignoring any failures (because there will definitely
   * be failures due to unsupported Installation types and missing Endpoints).
   *
   * @param parallel how many threads to run in parallel
   */
  List<SyncResult> synchronizeAllInstallations(int parallel);

  /**
   * Retrieve a count of records held in this dataset.
   *
   * @param dataset
   * @return the count, or null.
   */
  Long getDatasetCount(Dataset dataset, Endpoint endpoint) throws MetadataException;
}
