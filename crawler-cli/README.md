# GBIF Crawler CLI

The Crawler CLI (command-line interface) provides services that read and write RabbitMQ messages as well as talking
to Zookeeper in order to schedule, execute, and cleanup crawls. It uses the Crawler module to do the XML crawling and
does archive downloads directly. The XML pages or archives emitted as the last step of occurrence crawling are
consumed by Pipelines and Checklistbank.

**Crawling** is our process for downloading data from a remote system, potentially performing basic validation checks or
format conversions, updating dataset metadata (if provided), and archiving the data.  Except for metadata-only datasets
it is normally followed by processing, either by Pipelines (for datasets with occurrences), Checklistbank (for datasets
with taxa), or both.

Historically, crawling was combined with occurrence processing.  Since February 2023, successful crawling triggers
processing by pipelines, but is otherwise independent.  Checklist processing continues to share state with crawling.

## To build the project
```
mvn clean install
```

## Usual flow

1. One of the following happens:

   * A new dataset is registered with GBIF.  The registry waits 60 seconds in case of any further changes, then creates a StartCrawlMessage.
   * An existing dataset is updated.  The registry waits 60 seconds in case of any further changes, then creates a StartCrawlMessage.
   * A data manager or user with authorization triggers a crawl through the API, Registry UI, portal or CLI utility script.  The registry creates a StartCrawlMessage.
   * The crawl scheduler finds a dataset hasn't been crawled for over 7 days.  It creates a StartCrawlMessage.

   These messages go to the crawler-coordinator queue.

2. The crawler coordinator checks if the dataset is currently being crawled, and ignores the message if so.  It also
   checks for a suitable endpoint.  For DiGIR, TAPIR and BioCASe protocols, it retrieves the expected total
   (`declaredCount`) and stores it in ZooKeeper.  It then adds the dataset to a ZooKeeper queue,
   [dwca](https://api.gbif.org/v1/dataset/process/dwca/pending), [abcda](https://api.gbif.org/v1/dataset/process/abcda/pending)
   [camtrapdp](https://api.gbif.org/v1/dataset/process/camtrapdp/pending) or [xml](https://api.gbif.org/v1/dataset/process/xml/pending),
   though these are quickly processed and thus usually empty.

3. Crawl consumers (`AbcdaCrawlConsumer`, `CamtrapDpCrawlConsumer`, `DwcaCrawlConsumer`, `XmlCrawlConsumer`) read entries
   from these queues.  A new crawling state entry is created in ZooKeeper (`startedCrawling`), see [dataset/process/running](https://api.gbif.org/v1/dataset/process/running)
   or the [user interface](https://registry.gbif.org/monitoring/running-crawls).
   * Archive-based crawl consumers download the archive to NFS storage, making an HTTP conditional request based on the
     previously retrieved archive, if any.
     1. If the archive isn't modified, a hard link to the previous archive is created.  The crawl is marked as Finished: NOT_MODIFIED.
     2. If there's some HTTP error, the crawl is marked as Finished: ABORT.
     3. If something is retrieved, a `<TYPE>DownloadFinishedMessage` is created.
        * ABCD archives have no further validation. The crawl job is complete, and marked as such in ZooKeeper.  Pipelines picks up the AbcdaDownloadFinishedMessage.
        * Other archive types (Darwin Core, CamtrapDP) have crawler processes listening for their finish messages.

   * XML-based crawl consumers send requests to the source, and archive the results onto NFS storage.  When this is complete,
     a CrawlFinishedMessage is produced, which is picked up by Pipelines.

4. CamtrapDP archives are converted to Darwin Core Archives.  If this is successful, a DwcaDownloadFinishedMessage is
   produced, so the archive is then handled as below.

5. Darwin Core archives are validated.  The job is marked as complete (`NORMAL` or `ABORT`) in ZooKeeper.
   A DwcaValidationCompleteMessage is created.  Pipelines listens for these messages.
   * The Darwin Core metasync process also reads these messages.  Metadata may be updated even if the archive is otherwise
     invalid, and invalid metadata has no effect on crawl job state.  Checklistbank listens to the resulting
     DwcaMetasyncFinishedMessage.

6. Once all these crawling tasks are complete for any particular dataset, the crawl is noticed by the Cleanup process.
   The result is persisted to the Registry, and all state in ZooKeeper is removed.

## CLI targets
### scheduler
* Publishes `StartCrawlMessage` messages (routing key: "crawl.start").

### coordinator
* Listens to `StartCrawlMessage` messages.
* Creates and adds crawling jobs to crawling queues in ZooKeeper.

### crawlserver (XML)
* XML crawl server that listens to a Zookeeper queue ("xml").
* Publishes `CrawlResponseMessage` (routing key: "crawl.response").
* Publishes `CrawlFinishedMessage` (routing key: "crawl.finished") on finish.
* Publishes `CrawlErrorMessage` (routing key: "crawl.error" + errorType) on error.

### downloader (DwC-A)
* DwC-A crawl server that listens to a Zookeeper queue ("dwca").
* Publishes `DwcaDownloadFinishedMessage` (routing key: "crawl.dwca.download.finished") on success.

### abcdadownloader (ABCD-A)
* ABCD-A crawl server that listens to a Zookeeper queue ("abcda").
* Publishes `AbcdaDownloadFinishedMessage` (routing key: "crawl.abcda.download.finished") on success.

### camtrapdpdownloader (CamtrapDP)
* CamtrapDP crawl server that listens to a Zookeeper queue ("camtrapdp").
* Publishes `DwcaDownloadFinishedMessage` (routing key: "crawl.dwca.download.finished") on successful conversion
  of the data package to DwC-A format.

### validator (DwC-A)
* Listens to `DwcaDownloadFinishedMessage` messages.
* Publishes `DwcaValidationFinishedMessage` (routing key: "crawl.dwca.validation.finished") on success.

### dwca-metasync (DwC-A)
* Listens to `DwcaValidationFinishedMessage` messages.
* Publishes `DwcaMetasyncFinishedMessage` (routing key: "crawl.dwca.metasync.finished") on success.

### metasynceverything
* Publishes `StartMetasyncMessage` messages (routing key: "metasync.start").

### metasync
* Listen to `StartMetasyncMessage` messages.
* Supported protocols are: DiGIR, TAPIR and BioCASe.
