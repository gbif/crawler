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
package org.gbif.crawler.metasync.protocols.tapir.model.metadata;

import java.time.Duration;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.commons.digester3.annotations.rules.ObjectCreate;
import org.apache.commons.digester3.annotations.rules.SetProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.ToString;

@ObjectCreate(pattern = "response/metadata/indexingPreferences")
@ToString
public class IndexingPreferences {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingPreferences.class);

  @SetProperty(pattern = "response/metadata/indexingPreferences", attributeName = "startTime")
  private String startTime;

  private OffsetTime parsedStartTime;

  @SetProperty(pattern = "response/metadata/indexingPreferences", attributeName = "maxDuration")
  private Duration duration;

  @SetProperty(pattern = "response/metadata/indexingPreferences", attributeName = "frequency")
  private Duration frequency;

  public OffsetTime getParsedStartTime() {
    return parsedStartTime;
  }

  public String getStartTime() {
    return startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
    if (startTime == null) {
      this.parsedStartTime = null;
      return;
    }

    try {
      // Try parsing time with offset first, e.g. 13:00:00+01:00 or 13:00:00Z
      this.parsedStartTime = OffsetTime.parse(startTime, DateTimeFormatter.ISO_OFFSET_TIME);
    } catch (DateTimeParseException e1) {
      try {
        // Fallback: parse local time and attach UTC offset
        LocalTime lt = LocalTime.parse(startTime, DateTimeFormatter.ISO_TIME);
        this.parsedStartTime = lt.atOffset(ZoneOffset.UTC);
      } catch (DateTimeParseException e2) {
        LOG.debug("Could not parse time: [{}]", startTime);
        this.parsedStartTime = null;
      }
    }
  }

  public Duration getDuration() {
    return duration;
  }

  public void setDuration(Duration duration) {
    this.duration = duration;
  }

  public Duration getFrequency() {
    return frequency;
  }

  public void setFrequency(Duration frequency) {
    this.frequency = frequency;
  }

}
