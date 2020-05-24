# GBIF Crawler

This project is responsible for all the occurrence and checklist crawling. The coordinator, crawler, and cli modules work together to do the actual crawling.
The ws and ws-client present crawling status as recorded in zookeeper.

The Crawler project includes:
  1. [crawler](crawler/README.md): Contains the actual crawlers that speak the various XML and DWCA dialects needed for crawling the GBIF network
  2. [crawler-cleanup](crawler-cleanup/README.md): Used to delete crawl jobs in Zookeeper (see sub-module README for details how to use)
  3. [crawler-cli](crawler-cli/README.md): Provides the services that listen to RabbitMQ for instructions to crawl and fragment crawled resources
  4. [crawler-coordinator](crawler-coordinator/README.md): Coordinates crawling jobs via Zookeeper (Curator)
  5. [crawler-ws](crawler-ws/README.md): Exposes read only crawl status and access to logs. Affectionately called the CRAwling Monitor (CRAM)
  6. [crawler-ws-client](crawler-ws-client/README.md): Java client to the WS

## Building
See the individual sub-module READMEs for specific details, but in general it is enough to build all components with:

````shell
mvn clean package
````

## Sequence

### Darwin Core Archive
 * Downloader
   * Validator
     * Metasync
       * Fragmenter (Occurrence)
       * Normalizer (Checklist)

More information in crawler-cli [README](crawler-cli/README.md).

## Change Log
[Change Log](CHANGELOG.md)

## Note
  pipelines57 branch as branch for all changes related to Milestone for adding all DwCAvro files to avro (https://github.com/gbif/pipelines/projects/5)
