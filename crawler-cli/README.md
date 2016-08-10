# GBIF Crawler CLI

The Crawler CLI (command-line interface) provides services that read and write RabbitMQ messages as well as talking
to Zookeeper in order to schedule, execute, and cleanup crawls. It uses the Crawler module to do the XML crawling and
 does DWCA downloads directly. The occurrence fragments emitted as the last step of occurrence crawling are in turn
 consumed by services in occurrence-cli. Checklists are not fragmented but on successful crawls a message is sent which
 then triggers work in the checklistbank-cli.

## To build the project
```
mvn clean install
```

## CLI targets
### scheduler
* Publishes `StartCrawlMessage` messages (routing key: "crawl.start").

### coordinator
* Listen to `StartCrawlMessage` messages.
* Creates add crawling jobs to crawling queues in ZooKeeper.

### crawlserver (XML)
* XML crawl server that listens to a Zookeeper queue ("xml").
* Publishes `CrawlResponseMessage` (routing key: "crawl.response").
* Publishes `CrawlFinishedMessage` (routing key: "crawl.finished") on finish.
* Publishes `CrawlErrorMessage` (routing key: "crawl.error" + errorType) on error.

### fragmenter (XML)
* Listen to `CrawlResponseMessage` messages.
* Publishes `OccurrenceFragmentedMessage` (routing key: "crawler.fragment.new") on success.

### downloader (Dwc-A)
* Dwc-A crawl server that listens to a Zookeeper queue ("dwca").
* Publishes `DwcaDownloadFinishedMessage` (routing key: "crawl.dwca.download.finished") on success.

### validator (Dwc-A)
* Listen to `DwcaDownloadFinishedMessage` messages.
* Publishes `DwcaValidationFinishedMessage` (routing key: "crawl.dwca.validation.finished") on success.

### dwca-metasync (Dwc-A)
* Listen to `DwcaValidationFinishedMessage` messages.
* Publishes `DwcaMetasyncFinishedMessage` (routing key: "crawl.dwca.metasync.finished") on success.

### dwcafragmenter (Dwc-A)
* Listen to `DwcaMetasyncFinishedMessage` messages.
* Publishes `OccurrenceFragmentedMessage` (routing key: "crawler.fragment.new") on success.


### metasynceverything
Publishes `StartMetasyncMessage` messages (routing key: "metasync.start").

### metasync
Listen to `StartMetasyncMessage` messages.

Supported protocols are: Digir, Tapir and Biocase.


