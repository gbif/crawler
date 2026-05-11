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

### Darwin Core Data Package flow

Darwin Core Data Package processing uses the crawler CLI together with the separate
[dwc-dp-analyser-service](https://github.com/gbif/dwc-dp-analyser-service/tree/dev):

1. `dwcdpdownloader` reads DwC-DP crawl jobs from ZooKeeper, downloads archives to shared storage, and publishes
   `DwcDpDownloadFinishedMessage`.
2. `dwc-dp-analyser-service` is deployed from its own repository/chart. It consumes the download-finished message,
   validates the archive, and publishes `DwcDpValidationFinishedMessage`.
3. `dwcdp-metasync` consumes `DwcDpValidationFinishedMessage`, forwards metadata to the Registry, marks crawl state in
   ZooKeeper, and publishes `DwcDpMetadataSyncFinishedMessage` on success.

The downloader, validator, and metasync services must use compatible archive paths backed by the same shared storage
(normally NFS in shared environments).

### Catalogue of Life Data Package flow

1. `coldpdownloader` reads CoL-DP crawl jobs from ZooKeeper, downloads archives to shared storage, and publishes
   `ColDpDownloadFinishedMessage`.
2. `coldp-metasync` consumes `ColDpDownloadFinishedMessage`, forwards metadata to the Registry, and marks crawl state in
   ZooKeeper.

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

### dwcdpdownloader (Darwin Core Data Package)
* DwC-DP crawl server that listens to a ZooKeeper queue for Darwin Core Data Package crawl jobs.
* Downloads the archive to shared storage.
* Publishes `DwcDpDownloadFinishedMessage` on success.

### coldpdownloader (Catalogue of Life Data Package)
* CoL-DP crawl server that listens to a ZooKeeper queue for Catalogue of Life Data Package crawl jobs.
* Downloads the archive to shared storage.
* Publishes `ColDpDownloadFinishedMessage` on success.

### validator (DwC-A)
* Listens to `DwcaDownloadFinishedMessage` messages.
* Publishes `DwcaValidationFinishedMessage` (routing key: "crawl.dwca.validation.finished") on success.

### dwca-metasync (DwC-A)
* Listens to `DwcaValidationFinishedMessage` messages.
* Publishes `DwcaMetasyncFinishedMessage` (routing key: "crawl.dwca.metasync.finished") on success.

### dwcdp-metasync (Darwin Core Data Package)
* Listens to `DwcDpValidationFinishedMessage` messages published by the separate DWC-DP analyser service.
* Reads the downloaded DwC-DP archive from shared storage.
* Forwards datapackage/EML metadata to the Registry.
* Publishes `DwcDpMetadataSyncFinishedMessage` on success.

### coldp-metasync (Catalogue of Life Data Package)
* Listens to `ColDpDownloadFinishedMessage` messages.
* Reads the downloaded CoL-DP archive from shared storage.
* Forwards CoL-DP metadata to the Registry.

### metasynceverything
* Publishes `StartMetasyncMessage` messages (routing key: "metasync.start").

### metasync
* Listen to `StartMetasyncMessage` messages.
* Supported protocols are: DiGIR, TAPIR and BioCASe.

## Docker image

The root `Dockerfile` builds one reusable `crawler-cli` image. The image can run any CLI target by setting
`CRAWLER_COMMAND`.

The container entrypoint generates the CLI YAML configuration at startup from environment variables and command-specific
Helm values, then runs:

```shell
java -jar /app/crawler-cli.jar "$CRAWLER_COMMAND" --conf /app/.tmp/crawler.yaml
```

Explicit container arguments override the environment-driven command path, which is useful for debugging or one-off
manual runs.

## Kubernetes and Helm

The Helm chart in `helm/` is the preferred Kubernetes deployment structure. Commands are declared in
`helm/values.yaml` under `commands`.

Long-running listeners should use `workloadKind: Deployment`, for example:

```yaml
commands:
  dwcdp-metasync:
    enabled: true
    workloadKind: Deployment
    command: dwcdp-metasync
    config:
      archiveRepository: /data/dwcdp
      queueName: dwcdp.metasync
      poolSize: 1
    volumeMounts:
      - name: dwcdp
        mountPath: /data/dwcdp
```

One-off commands should use `workloadKind: Job`, for example:

```yaml
commands:
  startcrawl:
    enabled: true
    workloadKind: Job
    command: startcrawl
    config:
      datasetUuid: 00000000-0000-0000-0000-000000000000
      priority: 0
```

Use existing Kubernetes Secrets for RabbitMQ and Registry credentials in shared environments. The `createSecret` values
are intended for development convenience only.

Archive handoff requires shared storage. For production-like deployments, override the default `emptyDir` volumes with
NFS or another shared volume type so downloaders, validators, and metasync services see the same archive files.
