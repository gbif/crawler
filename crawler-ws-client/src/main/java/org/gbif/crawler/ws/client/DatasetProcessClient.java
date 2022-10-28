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
package org.gbif.crawler.ws.client;

import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.service.crawler.DatasetProcessService;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Client-side implementation to the crawler service.
 */
@RequestMapping("dataset/process")
public interface DatasetProcessClient extends DatasetProcessService {

  @RequestMapping(method = RequestMethod.GET, value = "detail/{datasetKey}", produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  @Override
  DatasetProcessStatus getDatasetProcessStatus(@PathVariable("datasetKey") UUID datasetKey);

  @RequestMapping(method = RequestMethod.GET, value = "running", produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  @Override
  Set<DatasetProcessStatus> getRunningDatasetProcesses();

  @RequestMapping(method = RequestMethod.GET, value = "xml/pending", produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  @Override
  List<DatasetProcessStatus> getPendingXmlDatasetProcesses();

  @RequestMapping(method = RequestMethod.GET, value = "dwca/pending", produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  @Override
  List<DatasetProcessStatus> getPendingDwcaDatasetProcesses();

}
