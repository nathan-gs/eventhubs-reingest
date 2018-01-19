# EventHubs Reingestion (Capture or other sources)

## Configuration

### Inputs
```
spark.eventhubsreingest.inputs.ALIAS.path="wasbs://..."
spark.eventhubsreingest.inputs.ALIAS.format=avro
spark.eventhubsreingest.inputs.ALIAS.options.KEY0=VALUE
spark.eventhubsreingest.inputs.ALIAS.options.KEY1=VALUE

# If query is not set we assume it's EventHubs Capture data. This will read the data from all avro paths.
spark.eventhubsreingest.query="SELECT * FROM ALIAS"

```

### Output

```
spark.eventhubsreingest.output.eh.ns=""
spark.eventhubsreingest.output.eh.name=""
spark.eventhubsreingest.output.eh.keyName=""
spark.eventhubsreingest.output.eh.keyValue=""

```

## Executing on HDInsight using livy

The `livy.sh` script will upload and submit the jar file. The `livy.sh` script assumes you have build the source code from scratch (`sbt assembly`).

```

export STORAGE_ACCOUNT_NAME="YOUR STORAGE ACC"
export STORAGE_CONTAINER_NAME="YOUR STORAGE CONTAINER"

export CLUSTER_URL="https://CLUSTERNAME.azurehdinsight.net"
export CLUSTER_USER="admin"
export CLUSTER_PASSWORD='A PASSWORD'

APP_CONF="\"spark.eventhubsreingest.inputs.capture.path\":\"wasbs://CAPTURE_CONTAINER@CAPTURE_STORAGE_ACC.blob.core.windows.net/PATH_TO_FILES/\""
APP_CONF="$APP_CONF,\"spark.eventhubsreingest.inputs.capture.format\":\"avro\""

APP_CONF="$APP_CONF,\"spark.eventhubsreingest.output.eh.ns\":\"EVENTHUB NAMESPACE\""
APP_CONF="$APP_CONF,\"spark.eventhubsreingest.output.eh.name\":\"EVENTHUB NAME\""
APP_CONF="$APP_CONF,\"spark.eventhubsreingest.output.eh.keyName\":\"RootManageSharedAccessKey\""
APP_CONF="$APP_CONF,\"spark.eventhubsreingest.output.eh.keyValue\":\"EVENTHUB Key"

export APP_CONF="$APP_CONF"

./livy.sh
```

Checking the status can be done through the Yarn UI, or using
`curl -k --user "$CLUSTER_USER:$CLUSTER_PASSWORD"` "$CLUSTER_URL/livy/batches"`

