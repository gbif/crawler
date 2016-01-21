# GBIF Crawler WebService

The Crawler Web Service provides the CRAwling Monitor (CRAM) which shows running crawls, their status, and gives links to logs in kibana. All of the count data comes from Zookeeper.

## To build the project

The following properties need to be set for running the webservice, typically provided in a properties file. For developing locally run with -Pdev using the profile from https://github.com/gbif/gbif-configuration/blob/master/maven/settings.xml.

- crawler.crawl.namespace
- crawler.crawl.server
- crawler.crawl.server.retryAttempts
- crawler.crawl.server.retryDelayMs
- crawler.crawl.threadCount

````
mvn clean install
````

To test locally:

````
mvn -Pdev jetty:run
````

and check on http://localhost:8080


