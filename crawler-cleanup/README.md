# GBIF Crawler Cleanup

The Crawler Cleanup project provides a way to delete crawl jobs from Zookeeper.

## To build the project (JAR)

The properties are hard-coded, so a properties file is not required in order to build the project. Simply run the following command to generate the JAR file:

````
mvn clean package
````

## To use the JAR

1. Choose the desired crawling environment where you want to execute the JAR from. E.g. uatcrawler-vh.gbif.org
2. Change to crap user: ```$ su - crap```
3. (Re)place the generated zookeeper-cleanup.jar inside ```/home/crap/util```
4. Ensure [crawl-cleanup bash script](https://github.com/gbif/gbif-configuration/blob/db721a709c2358c4b9c17f2f5f2da2fcc7bb85f2/cli/common/util/crawl-cleanup) exists inside ```/home/crap/util```
5. Create new text file with list of Dataset UUIDs. Each line/UUID must correspond to a dataset being crawled that you want to stop crawling. E.g.```$ echo 5e5934f6-5e18-4852-948f-97a64b17b0a8 > jobs_to_delete.txt```
6. Execute crawl-cleanup script specifying two ordered parameters: 1) path to UUIDs file, 2) desired environment ```$ ./crawl-cleanup jobs_to_delete.txt uat```
7. Using Crawling Monitor for desired environment, check crawl jobs have been deleted. E.g. http://crawler.gbif-uat.org/ for UAT environment.