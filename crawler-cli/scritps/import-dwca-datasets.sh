#!/bin/bash

# Pass as arguments
# EXAMPLE - source import-dwca-datasets.sh rpathak rpathak http://mq.gbif.org:15672/api/exchanges/%2Fusers%2Frpathak/crawler/publish
USER=$1
PASSWORD=$2
URL=$3

ROUTING_KEY="crawl.dwca.validation.finished"
CMD="ls -d */"

echo "-----RUN-----"

# All directories in the "dwca" directory
for UUID in `$CMD`; do

    UUID=${UUID%%/}

    # The RabbitMQ payload
    PAYLOAD="{\\\"datasetUuid\\\":\\\"$UUID\\\",\\\"datasetType\\\":\\\"OCCURRENCE\\\",\\\"source\\\":\\\"http://some.new.url\\\",\\\"attempt\\\":1,\\\"validationReport\\\":{\\\"datasetKey\\\":\\\"$UUID\\\",\\\"occurrenceReport\\\":null,\\\"genericReport\\\":null,\\\"invalidationReason\\\":\\\"no reason\\\",\\\"valid\\\":false}}"

    # Create curl command
    CURL="curl -u $USER:$PASSWORD -H \"content-type:application/json\" -X POST -d'{\"properties\":{\"delivery_mode\":1},\"routing_key\":\"$ROUTING_KEY\",\"payload\":\"$PAYLOAD\",\"payload_encoding\":\"string\"}' $URL"

    # Execute the HTTP request via curl
    $(eval $CURL)

done

echo "-----FINISHED-----"
