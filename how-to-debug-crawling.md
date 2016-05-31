# How to debug crawling

## Introduction

This page is designed to empower the GBIFS technical staff member to debug crawling by him or herself. 

It starts with an overview of all the services and infrastructure that can be utilized in debugging crawling:

* [Registry Console](how-to-debug-crawling.md#registry-console)
* [CRAwling Monitor (CRAM)](how-to-debug-crawling.md#crawling-monitor-cram)
* [Kibana](how-to-debug-crawling.md#kibana)
* [RabbitMQ (Queues)](how-to-debug-crawling.md#rabbitmq-queues)
* [Crawling Server](how-to-debug-crawling.md#crawling-server)

Note all services above are internal (not publicly accessible) except for [Kibana](how-to-debug-crawling.md#kibana).

Lastly, it gives a [case study](how-to-debug-crawling.md#case-study) illustrating how these services can collectively be used to debug crawling when it goes wrong. 

Debugging crawling is a complicated process. For finer details about the services' underlying components such as Zookeeper's structure, please refer to the section in GBIF's Confluence entitled [New GBIF Crawling and Occurrence Architecture](http://dev.gbif.org/wiki/display/DEV/New+GBIF+Crawling+and+Occurrence+Architecture).  

### Registry Console
|Environment| Address|
|---|---|
|PROD| http://registry.gbif.org|
|UAT| http://registry.gbif-uat.org|
|DEV| http://registry.gbif-dev.org|

Use the Registry Console to start a crawling job for a single dataset and evaluate one or more crawl summaries for a dataset. Note before triggering a new crawl, check the CRAM to make sure an existing crawling job isn't running for that dataset. If you trigger a new crawl, and get a ```NOT_MODIFIED``` status message, the crawl can be forced by removing the crawled content downloaded to the Crawling Server inside the Downloaded Content Directory.

### CRAwling Monitor (CRAM)
|Environment| Address|
|---|---|
|PROD| http://crawler.gbif.org/|
|UAT| http://crawler.gbif-uat.org/|
|DEV| http://crawler.gbif-dev.org/|

Use the CRAM to monitor crawling jobs while they are running. Filter jobs by Dataset key. Jobs will be removed upon completion. Check the Registry console to find the crawl summary. 

### Kibana 
|Environment| Address|
|---|---|
|ALL| http://kibana2.gbif.org (publicly accessible) |

Use Kibana to monitor what stage a crawling job is at, or to discover more detailed information such as why individual records weren't properly interpreted. Filter logs by Dataset key and environment. 

### RabbitMQ (Queues)
|Environment| Address| Virtual Host|
|---|---|---|
|PROD| http://mq.gbif.org:15672/#/queues|/prod|
|UAT| http://mq.gbif.org:15672/#/queues|/uat|
|DEV| http://mq.gbif.org:15672/#/queues|/dev|

Use RabbitMQ to monitor what stage a crawling job is at, and to ensure queues are being properly consumed - a sign the underlying crawling service is operating properly. Filter queues by environment (virtual host). Be careful queues don't get overloaded. For example if the mapping queue ```maps_z09``` gets backed-up, check its corresponding log file on the crawling server to make sure it hasn't blown memory: ```/home/crap/logs/maps_z09_stdout.log``` If memory has been blown, it may be necessary to delete all messages in the queues, otherwise the PROD environment can inadvertently be brought down.

### Crawling Server
|Environment| Address| Logs Directory| Downloaded Content Directory| Scripts Directory| Crawl-Cleaner Script Directory|
|---|---|---|---|---|---|
|PROD| prodcrawler-vh.gbif.org | /home/crap/logs| /mnt/auto/crawler/dwca| /home/crap/bin| /home/crap/util|
|UAT| uatcrawler-vh.gbif.org| /home/crap/logs| /mnt/auto/crawler/dwca| /home/crap/bin| /home/crap/util|
|DEV| devcrawler-vh.gbif.org| /home/crap/logs| /mnt/auto/crawler/dwca| /home/crap/bin| /home/crap/util|

Use the Crawling Server to monitor what crawling processes are running, start and stop the crawling (see Scripts Directory), delete crawling jobs (Crawl-Cleaner Script Directory), monitor logs of various crawling processes (see Logs Directory) and investigate the content downloaded from crawling (see Downloaded Content Directory). Warning: after SSH-ing onto the server, change to user crap ```su - crap``` before running any scripts. Warning2: the zookeeper-cleanup.jar on UAT needs to be rebuilt from the new [crawler-cleanup module](https://github.com/gbif/crawler/tree/master/crawler-cleanup). 

### Case Study

Imagine a crawling job is stalled at the stage of validating the downloaded DwC-A. 

To debug this case:

* start by checking the [Registry Console](how-to-debug-crawling.md#registry-console) to see if the crawl finished. 
* use the [CRAwling Monitor (CRAM)](how-to-debug-crawling.md#crawling-monitor-cram) to check the job is still running. 
* check the [RabbitMQ (Queues)](how-to-debug-crawling.md#rabbitmq-queues) to see if messages are building up in some queues (in this example, imagine the dwca-validator queue is backed-up).
* open the queue to see if the consumers are actually operating and listening to the queue (in this example, imagine they aren't).
* SSH onto the [Crawling Server](how-to-debug-crawling.md#crawling-server) and quickly change to user "crap"
* check the crawling processes are up, specifically check the validator process is running: ```ps -ef | grep valid```
* tail the validator service's logs to determine if it's failed for some particular reason: ```tail -f crawler-dwca-validator.log``` (in this example, imagine it is idle and thus needs to be restarted)
* restart the crawling services: ```./stop-all.sh``` & ```./start-all.sh```
* use [Kibana](how-to-debug-crawling.md#kibana) to monitor crawling is moving through the various stages
