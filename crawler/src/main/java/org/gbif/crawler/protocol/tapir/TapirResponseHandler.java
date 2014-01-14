package org.gbif.crawler.protocol.tapir;

import org.gbif.crawler.exception.ProtocolException;
import org.gbif.crawler.protocol.AbstractResponseHandler;

import javax.annotation.concurrent.NotThreadSafe;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import com.google.common.base.Optional;
import org.codehaus.stax2.XMLStreamReader2;

/**
 * This class handles a TAPIR response.
 * <p/>
 * Every TAPIR response has a few pieces of diagnostic information as defined in the <a
 * href="http://www.tdwg.org/dav/subgroups/tapir/1.0/docs/tdwg_tapir_specification_2010-05-05.htm">specification</a>:
 * <ul>
 * <li>{@code start} index of the first element in this response</li>
 * <li>{@code totalReturned} number of elements in this response</li>
 * <li>{@code next} index of the next element that should be retrieved. Used for paging. This attribute is absent if
 * there are no more records.</li>
 * </ul>
 * <p/>
 * As an example. Sending a request with {@code start}=0 and {@code limit}=2 could return a response like this:
 * <pre>
 *   {@code <summary start='0' next='2' totalReturned='2'/>}
 * </pre>
 * <p/>
 * This class is not thread-safe.
 */
@NotThreadSafe
public class TapirResponseHandler extends AbstractResponseHandler {

  private static final String CONTENT_ELEMENT = "search";
  private static final String INFO_ELEMENT = "summary";

  private boolean insideContent;
  private boolean seenContent;

  @Override
  protected boolean shouldHash(XMLStreamReader2 reader) {
    if (isElement(reader, CONTENT_ELEMENT, true)) {
      insideContent = true;
      return false;
    }

    boolean infoElement = isElement(reader, INFO_ELEMENT, true) || isElement(reader, INFO_ELEMENT, false);

    if (insideContent && !infoElement && reader.getEventType() == XMLStreamConstants.START_ELEMENT) {
      seenContent = true;
    }

    if (isElement(reader, CONTENT_ELEMENT, false)) {
      insideContent = false;
    }

    return insideContent && seenContent && !infoElement;
  }

  @Override
  protected void initialize() {
    insideContent = false;
    seenContent = false;
  }

  @Override
  protected void process(XMLStreamReader2 reader) throws XMLStreamException, ProtocolException {
    if (isElement(reader, INFO_ELEMENT, true)) {
      int index = reader.getAttributeIndex(null, "totalReturned");
      if (index == -1) {
        setRecordCount(Optional.<Integer>absent());
      } else {
        setRecordCount(Optional.of(reader.getAttributeAsInt(index)));
      }

      if (reader.getAttributeIndex(null, "next") == -1) {
        setEndOfRecords(Optional.<Boolean>absent());
      } else {
        setEndOfRecords(Optional.of(false));
      }

    }
  }

}
