# GBIF Crawler

This project is responsible for all the occurrence and checklist crawling. The coordinator, crawler, and cli modules work together to do the actual crawling. The ws and ws-client present crawling status as recorded in zookeeper.

The Crawler project includes:
  1. crawler: Contains the actual crawlers that speak the various XML and DWCA dialects needed for crawling the GBIF network
  2. crawler-cleanup: Used to delete crawl jobs in Zookeeper (see sub-module README for details how to use)
  3. crawler-cli: Provides the services that listen to RabbitMQ for instructions to crawl and fragment crawled resources
  4. crawler-coordinator: Coordinates crawling jobs via Zookeper (Curator)
  5. crawler-ws: Exposes read only crawl status and access to logs. Affectionately called the CRAwling Monitor (CRAM)
  6. crawler-ws-client: Java client to the WS

## Building
See the individual sub-module READMEs for specific details, but in general it is enough to build all components with:

````shell
mvn clean package
````
