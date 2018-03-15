package org.gbif.crawler.pipelines.dwca;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage;
import org.gbif.pipelines.core.io.DwCAReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Call back which is called when the {@link org.gbif.common.messaging.api.messages.DwcaValidationFinishedMessage } is received.
 */
public class DwCAToAvroCallBack extends AbstractMessageCallback<DwcaValidationFinishedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(DwCAToAvroCallBack.class);
  private final DwCAToAvroConfiguration configuration;

  DwCAToAvroCallBack(DwCAToAvroConfiguration configuration) {
    Objects.requireNonNull(configuration, "Configuration cannot be null");
    this.configuration = configuration;
  }

  @Override
  public void handleMessage(DwcaValidationFinishedMessage dwcaValidationFinishedMessage) {
    LOG.info("Received Download finished validation message {}", dwcaValidationFinishedMessage.toString());
    LOG.info("Verifying the configuration parameters for dataset {}, before exporting",
             dwcaValidationFinishedMessage.getDatasetUuid());
    DwCAToAvroCommandVerification.DwCA2AvroConfigurationParameter configParameter =
      DwCAToAvroCommandVerification.of(configuration)
        .with(dwcaValidationFinishedMessage)
        .verifyParametersAndGetResourceConfigurations();

    DatumWriter<ExtendedRecord> writer = new SpecificDatumWriter<>();

    try (DataFileWriter<ExtendedRecord> dataFileWriter = new DataFileWriter<>(writer)) {
      OutputStream os = configParameter.getFs().create(configParameter.getAbsoluteDatasetExportPath());
      dataFileWriter.create(ExtendedRecord.getClassSchema(), os);
      LOG.info("Extracting the DwC Archive {} started", configParameter.getAbsoluteDwCAPath());
      DwCAReader reader =
        new DwCAReader(configParameter.getAbsoluteDwCAPath(), configParameter.getAbsoluteDwCAExportPath());
      reader.init();
      LOG.info("Exporting the DwC Archive to avro {} started", configParameter.getAbsoluteDatasetExportPath());
      while (reader.advance()) dataFileWriter.append(reader.getCurrent());

    } catch (IOException e) {
      LOG.error("Failed performing conversion on " + dwcaValidationFinishedMessage.getDatasetUuid(), e);
      throw new RuntimeException("Failed performing conversion on " + dwcaValidationFinishedMessage.getDatasetUuid(),
                                 e);
    }
    LOG.info("DwCA to avro conversion completed for {}", dwcaValidationFinishedMessage.getDatasetUuid());
  }

}
