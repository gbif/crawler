# GBIF Crawler Coordinator

The Crawler Coordinator tracks crawls from beginning to end using counts updated in ZooKeeper. Those counts are what the crawler-ws displays. The coordinator listens for messages to start crawls and then adds the crawl to ZooKeeper and waits for updates to that entry. There is logic to ensure datasets can't be crawled multiple times concurrently as well as setting priority when multiple crawlable endpoints are available for a given dataset. This is a library, run by crawler-cli services.

## To build the project
```
mvn clean install
```



