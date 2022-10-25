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
package org.gbif.crawler.protocol.digir;

import org.gbif.crawler.exception.FatalCrawlException;
import org.gbif.crawler.protocol.AbstractResponseHandler;

import java.util.Optional;

import javax.annotation.concurrent.NotThreadSafe;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.codehaus.stax2.XMLStreamReader2;


/**
 * This class is not thread-safe.
 *
 * <p>The latest <a href="http://digir.sourceforge.net/schema/protocol/2003/1.0/digir.xsd">DiGIR
 * XSD</a> does not require any content or diagnostics elements.
 *
 * <p>So with DiGIR it could be that we successfully parse a page and are in a valid state but don't
 * know if we're at the end of the records or how many we got.
 */
@NotThreadSafe
public class DigirResponseHandler extends AbstractResponseHandler {

  private static final String CONTENT_ELEMENT = "content";
  private static final String INFO_ELEMENT = "diagnostic";

  private static final String DIAGNOSITC_ELEMENT_ATTRIBUTE_NAME = "code";

  private static final String RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND";
  private static final String RECORD_COUNT = "RECORD_COUNT";
  private static final String END_OF_RECORDS = "END_OF_RECORDS";

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
  protected void process(XMLStreamReader2 reader) throws XMLStreamException, FatalCrawlException {
    if (isElement(reader, INFO_ELEMENT, true)) {
      if (reader.getAttributeValue(null, DIAGNOSITC_ELEMENT_ATTRIBUTE_NAME).equals(RECORD_COUNT)) {
        setRecordCount(Optional.of(reader.getElementAsInt()));
      } else if (reader
          .getAttributeValue(null, DIAGNOSITC_ELEMENT_ATTRIBUTE_NAME)
          .equals(END_OF_RECORDS)) {
        setEndOfRecords(Optional.of(reader.getElementAsBoolean()));
      } else if (reader
          .getAttributeValue(null, DIAGNOSITC_ELEMENT_ATTRIBUTE_NAME)
          .equals(RESOURCE_NOT_FOUND)) {
        throw new FatalCrawlException("Requested resource could not be found");
      }
    }
  }
}
