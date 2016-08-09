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
Publishes `StartCrawlMessage` messages (routing key: "crawl.start").

### coordinator
Listen to `StartCrawlMessage` messages.

### metasynceverything
Publishes `StartMetasyncMessage` messages (routing key: "metasync.start").


### metasync
Listen to `StartMetasyncMessage` messages.

Supported protocols are: Digir, Tapir and Biocase.


