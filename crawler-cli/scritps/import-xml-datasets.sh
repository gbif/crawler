#!/bin/bash

# Pass as arguments
# EXAMPLE - source import-xml-datasets.sh mlopez mlopez http://mq.gbif.org:15672/api/exchanges/%2Fusers%2Fmlopez/crawler/publish
USER=$1
PASSWORD=$2
URL=$3

ROUTING_KEY="crawl.finished"
CMD="ls"


echo "-----RUN-----"

# All directories in the "xml" directory
for UUID in `$CMD`; do

    # Calculate last attempt
    ATTEMPT=0
    for b in `$CMD/$UUID | cut -d '.' -f 1`; do
      if [ $b -gt $ATTEMPT ]
      then
        ATTEMPT=$b
      fi
    done

    # The RabbitMQ payload
    PAYLOAD="{\\\"datasetUuid\\\":\\\"$UUID\\\",\\\"attempt\\\":\\\"$ATTEMPT\\\",\\\"totalRecordCount\\\":\\\"0\\\",\\\"reason\\\":\\\"NORMAL\\\"}"

    # Create curl command
    CURL="curl -u $USER:$PASSWORD -H \"content-type:application/json\" -X POST -d'{\"properties\":{\"delivery_mode\":1},\"routing_key\":\"$ROUTING_KEY\",\"payload\":\"$PAYLOAD\",\"payload_encoding\":\"string\"}' $URL"

    # Execute the HTTP request via curl
    $(eval $CURL)

done

echo "-----FINISHED-----"
