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