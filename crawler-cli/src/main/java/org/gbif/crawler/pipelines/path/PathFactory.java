package org.gbif.crawler.pipelines.path;

import static org.gbif.crawler.pipelines.path.PathFactory.ArchiveTypeEnum.DWCA;
import static org.gbif.crawler.pipelines.path.PathFactory.ArchiveTypeEnum.XML;

public class PathFactory {

  public enum ArchiveTypeEnum {
    XML, DWCA
  }

  private PathFactory() {
  }

  public static ArchiveToAvroPath create(ArchiveTypeEnum typeEnum) {
    if (XML == typeEnum) {
      return new XmlToAvroPath();
    }
    if (DWCA == typeEnum) {
      return new DwCAToAvroPath();
    }
    throw new IllegalArgumentException("Did not find archive type enum - " + typeEnum);
  }

}
