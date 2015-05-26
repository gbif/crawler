package org.gbif.crawler.emlpusher;

import org.gbif.api.service.registry.DatasetService;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;
import org.gbif.dwca.io.UnsupportedArchiveException;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.ws.client.guice.SingleUserAuthModule;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility that will inspect a crawling filesystem and push all metadata documents found into the registry.
 */
public class EmlPusher {

  private static final Logger LOG = LoggerFactory.getLogger(EmlPusher.class);
  private static final String dwcaSuffix = ".dwca";
  private int pushCounter;
  private int failCounter;
  private final File rootDirectory;
  private final DatasetService datasetService;

  public static EmlPusher build(PushEmlConfiguration cfg) {
    return new EmlPusher(cfg);
  }

  private EmlPusher(PushEmlConfiguration cfg) {
    LOG.info("Connecting to registry {} as user {}", cfg.registryWsUrl, cfg.registryUser);

    Properties p = new Properties();
    p.put("registry.ws.url", cfg.registryWsUrl);
    Injector inj = Guice
      .createInjector(new SingleUserAuthModule(cfg.registryUser, cfg.registryPassword), new RegistryWsClientModule(p));
    datasetService = inj.getInstance(DatasetService.class);

    rootDirectory = cfg.archiveRepository;
  }

  public void pushAll() {
    pushCounter = 0;
    File[] archiveFiles = findArchives(rootDirectory);
    LOG.info("Found {} archives", archiveFiles.length);

    for (File f : archiveFiles) {
      push(f);
    }
    LOG.info("Done. {} metadata documents from {} archives pushed to registry, {} failed", pushCounter,
      archiveFiles.length, failCounter);
  }

  private void pushEMl(UUID key, File eml) throws IOException {
    InputStream in = new FileInputStream(eml);
    try {
      datasetService.insertMetadata(key, in);
      LOG.info("Pushed metadata document for dataset {} into registry", key);
      pushCounter++;
    } finally {
      in.close();
    }
  }

  private void push(File archiveFile) {
    UUID key = getDatasetKey(archiveFile);
    try {
      Archive arch = open(archiveFile);
      File eml = arch.getMetadataLocationFile();
      if (eml != null && eml.exists()) {
        pushEMl(key, eml);
      }
    } catch (UnsupportedArchiveException e) {
      LOG.warn("Skipping archive {} because of error[{}]", key, e.getMessage());
      failCounter++;

    } catch (Exception e) {
      LOG.error("Unexpected exception when pushing metadata for dataset {}: {}", key, e);
      failCounter++;
    }
  }

  private UUID getDatasetKey(File archiveFile) {
    File dir = archiveDir(archiveFile);
    return UUID.fromString(dir.getName());
  }

  private File archiveDir(File archiveFile) {
    return new File(archiveFile.getParentFile(), FilenameUtils.getBaseName(archiveFile.getName()));
  }

  private Archive open(File archiveFile) throws UnsupportedArchiveException {
    // does the folder already exist (it should have the same name as the archive)?
    File dir = archiveDir(archiveFile);
    try {
      if (dir.exists()) {
        return ArchiveFactory.openArchive(dir);
      } else {
        LOG.info("Decompress archive {}", archiveFile.getAbsoluteFile());
        return ArchiveFactory.openArchive(archiveFile, dir);
      }
    } catch (IOException e) {
      throw new UnsupportedArchiveException(e);
    }
  }

  // gets the list of archives
  private File[] findArchives(File rootDirectory) {
    return rootDirectory.listFiles((FileFilter) new SuffixFileFilter(dwcaSuffix, IOCase.INSENSITIVE));
  }

}