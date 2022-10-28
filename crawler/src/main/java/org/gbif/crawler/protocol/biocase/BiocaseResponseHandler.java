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
package org.gbif.crawler.protocol.biocase;

import org.gbif.crawler.exception.ProtocolException;
import org.gbif.crawler.protocol.AbstractResponseHandler;

import java.util.Optional;

import javax.annotation.concurrent.NotThreadSafe;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.codehaus.stax2.XMLStreamReader2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles a BioCASe response.
 *
 * <p>Every BioCASe response has a few pieces of optional diagnostic information:
 *
 * <ul>
 *   <li>{@code recordCount} is the number of records that are being returned in this response
 *   <li>{@code recordDropped} is the number of dropped records for this response, this number is
 *       <em>not</em> included in the {@code recordCount}s.
 *   <li>{@code recordStart} is the {@code offset} that was used for this request
 *   <li>{@code totalSearchHits} is the total number of results for this query. It usually doesn't
 *       return an exact count as that is expensive to calculate. If {@code totalSearchHits} is
 *       greater than {@code recordCount} + {@code recordDropped} + {@code recordStart} there should
 *       be more results
 * </ul>
 *
 * <p>As an example. Sending a request with {@code start}=0 and {@code limit}=100 could return a
 * response like this:
 *
 * <pre>
 *   {@code <biocase:content recordCount='98' recordDropped='2' recordStart='0' totalSearchHits='101'>}
 * </pre>
 *
 * <p>Which would mean that the response includes 98 records and 2 had to be dropped on the way out.
 * Because totalSearchHits is greater than recordCount + recordDropped + recordStart there are more
 * pages and the next request should be with {@code start}=100 and {@code limit}=100 which could
 * return this:
 *
 * <pre>
 *   {@code <biocase:content recordCount='80' recordDropped='5' recordStart='100' totalSearchHits='185'>}
 * </pre>
 *
 * <p>This class is not thread-safe.
 */
@NotThreadSafe
public class BiocaseResponseHandler extends AbstractResponseHandler {

  private static final Logger LOG = LoggerFactory.getLogger(BiocaseResponseHandler.class);

  private static final String CONTENT_ELEMENT = "content";

  private boolean insideContent;
  private boolean seenContent;

  @Override
  protected boolean shouldHash(XMLStreamReader2 reader) {
    if (isElement(reader, CONTENT_ELEMENT, true)) {
      insideContent = true;
      return false;
    }

    if (insideContent && reader.getEventType() == XMLStreamConstants.START_ELEMENT) {
      seenContent = true;
    }

    // We found the end element which shouldn't be hashed either
    if (isElement(reader, CONTENT_ELEMENT, false)) {
      insideContent = false;
    }

    return insideContent && seenContent;
  }

  @Override
  protected void initialize() {
    insideContent = false;
    seenContent = false;
  }

  @Override
  protected void process(XMLStreamReader2 reader) throws XMLStreamException, ProtocolException {
    if (reader.getEventType() == XMLStreamConstants.START_ELEMENT
        && reader.getLocalName().equals(CONTENT_ELEMENT)) {
      Optional<Integer> recordStart = getAttribute(reader, "recordStart");
      Optional<Integer> recordCount = getAttribute(reader, "recordCount");
      Optional<Integer> recordDropped = getAttribute(reader, "recordDropped");
      Optional<Integer> totalHits = getAttribute(reader, "totalSearchHits");

      setRecordCount(recordCount);
      if (recordCount.isPresent()
          && recordStart.isPresent()
          && recordDropped.isPresent()
          && totalHits.isPresent()) {
        setEndOfRecords(
            Optional.of(
                recordCount.get() + recordStart.get() + recordDropped.get() >= totalHits.get()));
      }

      LOG.debug(
          "Found "
              + CONTENT_ELEMENT
              + " element. [{}] records, [{}] records dropped, [{}] record start, [{}] total search hits",
          recordCount, recordDropped, recordStart, totalHits);
    }
  }

  /**
   * This tries to read an attribute of an element as an integer and returns it if found, {@link
   * Optional#empty()} otherwise.
   *
   * @param reader to read the attribute from, needs to be set to an element
   * @param attribute to read from the element
   * @return an Optional with the integer value of the attribute {@code absent} otherwise
   */
  private Optional<Integer> getAttribute(XMLStreamReader2 reader, String attribute)
      throws XMLStreamException {
    int index = reader.getAttributeIndex(null, attribute);
    if (index == -1) {
      return Optional.empty();
    }

    return Optional.of(reader.getAttributeAsInt(index));
  }
}
