#!/bin/bash

# STORAGE_ACCOUNT_NAME
# STORAGE_CONTAINER_NAME
# CLUSTER_URL
# CLUSTER_USER
# CLUSTER_PASSWORD
# APP_CONF

JAR_FILE=`find target/scala-2.11/ -name 'eventhubs-reingest-assembly-*.jar' | head -n1 | xargs basename`
STORAGE_PATH="livy-submit/$JAR_FILE"

az storage blob upload \
  --file target/scala-2.11/$JAR_FILE \
  --container-name $STORAGE_CONTAINER_NAME \
  --account-name $STORAGE_ACCOUNT_NAME \
  --name $STORAGE_PATH

JAR_URL="wasbs://$STORAGE_CONTAINER_NAME@$STORAGE_ACCOUNT_NAME.blob.core.windows.net/$STORAGE_PATH"

# JAR_URL="wasbs:///$STORAGE_CONTAINER_NAME/$STORAGE_PATH"


LIVY_CREATE_JSON="{\"file\": \"$JAR_URL\", \"conf\":{$APP_CONF},\"className\":\"gs.nathan.eventhubsreingest.Main\"}"

curl -k --user "$CLUSTER_USER:$CLUSTER_PASSWORD" \
    -H "Content-Type: application/json" \
    -X POST \
    --data "$LIVY_CREATE_JSON" \
    "$CLUSTER_URL/livy/batches"