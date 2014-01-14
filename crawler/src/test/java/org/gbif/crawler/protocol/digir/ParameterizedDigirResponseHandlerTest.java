package org.gbif.crawler.protocol.digir;

import org.gbif.crawler.ResponseHandler;
import org.gbif.crawler.protocol.BaseParameterizedResponseHandlerTest;

import java.io.IOException;
import java.util.Collection;

import com.google.common.base.Optional;
import org.apache.http.HttpResponse;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ParameterizedDigirResponseHandlerTest extends BaseParameterizedResponseHandlerTest {

  public ParameterizedDigirResponseHandlerTest(String fileName, Optional<Integer> recordCount, Optional<Long> contentHash,
                                               Optional<Boolean> endOfRecords, boolean expectedException) {
    super(fileName, recordCount, contentHash, endOfRecords, expectedException);
  }

  @Override
  protected ResponseHandler<HttpResponse, ?> getResponseHandler() {
    return new DigirResponseHandler();
  }

  @Parameters(name = "{index}: {0}")
  public static Collection<Object[]> data() throws IOException {
    return getTestData("org/gbif/crawler/protocol/digir/responses.json");
  }
}
