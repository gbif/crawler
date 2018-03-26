package org.gbif.crawler.common;

import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import org.apache.avro.file.CodecFactory;

public class AvroWriteConfiguration {

  @Parameter(names = "--compression-type")
  @NotNull
  public String compressionType = "SNAPPY";

  @Parameter(names = "--sync-interval")
  @NotNull
  public int syncInterval = 2 * 1024 * 1024;

  public CodecFactory getCodec() {
    return Codecs.valueOf(compressionType.toUpperCase()).getCodec();
  }

  private enum Codecs {
    SNAPPY(CodecFactory.snappyCodec()), DEFLATE_1(CodecFactory.deflateCodec(1)), DEFLATE_2(CodecFactory.deflateCodec(2)), DEFLATE_3(
      CodecFactory.deflateCodec(3)), DEFLATE_4(CodecFactory.deflateCodec(4)), DEFLATE_5(CodecFactory.deflateCodec(5)), DEFLATE_6(
      CodecFactory.deflateCodec(6)), DEFLATE_7(CodecFactory.deflateCodec(7)), DEFLATE_8(CodecFactory.deflateCodec(8)), DEFLATE_9(
      CodecFactory.deflateCodec(9)), NULL_CODEC(CodecFactory.nullCodec());
    private final CodecFactory codec;

    Codecs(CodecFactory codec) {
      this.codec = codec;
    }

    public CodecFactory getCodec() {
      return codec;
    }
  }
}
