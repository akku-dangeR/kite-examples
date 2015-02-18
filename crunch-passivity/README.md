
# Kite Crunch passivity Example

## Running
Note: The example works only with CDH4

Build the code with:

```bash
mvn clean install
```

Then create the dataset and write some data to it with:

```bash
hadoop jar crunch-passivity-0.18.0.jar org.kitesdk.examples.data.WriteUserDataset
```

Read some from the dataset:

```bash
hadoop jar crunch-passivity-0.18.0.jar org.kitesdk.examples.data.ReadUserDataset
```

## Schema Evolution

Copy the contents of _src/main/avro/user.avsc.valid-migration_ to
_src/main/avro/user.avsc_, update the dataset's schema, and load new items:

```bash
cp src/main/avro/user.avsc.invalid-migration src/main/avro/user.avsc.old-schema

mvn clean install

hadoop jar crunch-passivity-0.18.0.jar org.kitesdk.examples.data.WriteUserDataset
```

At this point, the dataset is updated with a new schema. Reading data from the dataset
with same schema will execute successfully. 

```bash
hadoop jar crunch-passivity-0.18.0.jar org.kitesdk.examples.data.ReadUserDataset
```
However, if I use a old schema to read from the dataset the crunch job fails.

```bash
cp src/main/avro/user.avsc.old-schema src/main/avro/user.avsc

mvn clean install

hadoop jar crunch-passivity-0.18.0.jar org.kitesdk.examples.data.ReadUserDataset
```
