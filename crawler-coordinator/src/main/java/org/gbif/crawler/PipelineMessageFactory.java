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

import org.gbif.api.model.metrics.cube.OccurrenceCube;
import org.gbif.api.model.metrics.cube.ReadBuilder;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.service.metrics.CubeService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.util.MachineTagUtils;
import org.gbif.api.util.comparators.EndpointCreatedComparator;
import org.gbif.api.util.comparators.EndpointPriorityComparator;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.gbif.api.vocabulary.TagName.CRAWL_ATTEMPT;

public class PipelineMessageFactory {

  private static final Comparator<Endpoint> ENDPOINT_COMPARATOR =
      Ordering.compound(
          Lists.newArrayList(
              Collections.reverseOrder(new EndpointPriorityComparator()),
              EndpointCreatedComparator.INSTANCE));

  private final DatasetService datasetService;
  private final CubeService cubeService;

  public PipelineMessageFactory(DatasetService datasetService, CubeService cubeService) {
    this.datasetService = datasetService;
    this.cubeService = cubeService;
  }

  /**
   * Gets the endpoint that we want to crawl from the passed in dataset.
   *
   * <p>We take into account a list of supported and prioritized endpoint types and verify that the
   * declared dataset type matches a supported endpoint type.
   *
   * @param dataset to get the endpoint for
   * @return will be present if we found an eligible endpoint
   * @see EndpointPriorityComparator
   */
  private Optional<Endpoint> getEndpointToCrawl(Dataset dataset) {
    // Are any of the endpoints eligible to be crawled
    List<Endpoint> sortedEndpoints = prioritySortEndpoints(dataset.getEndpoints());
    if (sortedEndpoints.isEmpty()) {
      return Optional.empty();
    }
    Endpoint ep = sortedEndpoints.get(0);
    return Optional.ofNullable(ep);
  }

  /**
   * Returns a list of valid Endpoints for the currently available Crawler implementations in a
   * priority sorted order.
   *
   * @param endpoints to sort
   * @return sorted and filtered list of Endpoints with the <em>best</em> ones at the beginning of
   *     the list
   * @see EndpointPriorityComparator
   */
  @VisibleForTesting
  List<Endpoint> prioritySortEndpoints(List<Endpoint> endpoints) {
    checkNotNull(endpoints, "endpoints can't be null");

    // Filter out all Endpoints that we can't crawl
    List<Endpoint> result = new ArrayList<>();
    for (Endpoint endpoint : endpoints) {
      if (EndpointPriorityComparator.PRIORITIES.contains(endpoint.getType())) {
        result.add(endpoint);
      }
    }

    // Sort the remaining ones
    result.sort(ENDPOINT_COMPARATOR);
    return result;
  }

  /** Use the metrics web service to get the record count of a dataset. */
  private long getMetricsRecordCount(UUID dataseyKey) {
    return cubeService.get(new ReadBuilder().at(OccurrenceCube.DATASET_KEY, dataseyKey));
  }

  /**
   * This retrieves the current attempt of crawling for this dataset, increments it by one (in the
   * registry) and returns this new number.
   */
  private int getAttempt(UUID datasetKey, Dataset dataset, Endpoint endpoint) {
    int attempt = 0;

    // Find the maximum last attempt ID if any exist, deleting all others
    for (MachineTag tag : MachineTagUtils.list(dataset, CRAWL_ATTEMPT)) {
      int tagKey = tag.getKey();
      try {
        int taggedAttempt = Integer.parseInt(tag.getValue());
        attempt = Math.max(taggedAttempt, attempt);
      } catch (NumberFormatException e) {
        // Swallow it - the tag is corrupt and should be removed
      }
      datasetService.deleteMachineTag(datasetKey, tagKey);
    }
    // store updated tag
    attempt++;
    MachineTag tag =
        new MachineTag(
            CRAWL_ATTEMPT.getNamespace().getNamespace(),
            CRAWL_ATTEMPT.getName(),
            String.valueOf(attempt));
    datasetService.addMachineTag(datasetKey, tag);

    // metrics.registerCrawl(endpoint);
    return attempt;
  }

  public PipelinesDwcaMessage buildPipelinesDwcaMessage(UUID datasetKey, Set<String> steps) {
    Dataset dataset = datasetService.get(datasetKey);
    PipelinesDwcaMessage message = new PipelinesDwcaMessage();
    message.setDatasetType(DatasetType.OCCURRENCE);
    message.setDatasetUuid(datasetKey);
    message.setPipelineSteps(steps);
    getEndpointToCrawl(dataset)
        .ifPresent(
            endpoint -> {
              message.setEndpointType(endpoint.getType());
              message.setSource(endpoint.getUrl());
              message.setAttempt(getAttempt(datasetKey, dataset, endpoint));
            });
    return message;
  }

  public PipelinesInterpretedMessage buildPipelinesInterpretedMessage(
      UUID datasetKey, Set<String> steps) {
    Dataset dataset = datasetService.get(datasetKey);
    PipelinesInterpretedMessage message = new PipelinesInterpretedMessage();
    message.setDatasetUuid(datasetKey);
    message.setNumberOfRecords(getMetricsRecordCount(datasetKey));
    getEndpointToCrawl(dataset)
        .ifPresent(endpoint -> message.setAttempt(getAttempt(datasetKey, dataset, endpoint)));
    return message;
  }

  public PipelinesXmlMessage buildPipelinesXmlMessage(UUID datasetKey, Set<String> steps) {
    Dataset dataset = datasetService.get(datasetKey);
    PipelinesXmlMessage message = new PipelinesXmlMessage();
    message.setDatasetUuid(datasetKey);
    message.setTotalRecordCount(Long.valueOf(getMetricsRecordCount(datasetKey)).intValue());
    getEndpointToCrawl(dataset)
        .ifPresent(
            endpoint -> {
              message.setEndpointType(endpoint.getType());
              message.setAttempt(getAttempt(datasetKey, dataset, endpoint));
            });
    return message;
  }

  public static PipelinesBalancerMessage buildPipelinesBalancerMessage(
      Class<?> messageClass, PipelineBasedMessage payload) {
    PipelinesBalancerMessage message = new PipelinesBalancerMessage();
    message.setMessageClass(messageClass.getSimpleName());
    message.setPayload(payload.toString());
    return message;
  }
}
