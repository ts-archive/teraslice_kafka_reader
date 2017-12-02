# Reader - teraslice_kafka_reader

To install from the root of your teraslice instance.

```
npm install terascope/teraslice_kafka_reader
```

# Description

Teraslice reader for processing data from kafka topics.

# Output

An array of records from Kafka. Array may be up to `size` in length. No additional processing is done on the records.

# Parameters

| Name | Description | Default | Required |
| ---- | ----------- | ------- | -------- |
| topic | Name of the Kafka topic to process |  | Y |
| group | Name of the Kafka consumer group | | Y |
| connection | The Kafka consumer connection to use | | Y |
| size | How many records to read before a slice is considered complete | 10000 | N |
| wait | How long to wait for a full chunk of data to be available. Specified in milliseconds. | 30000 | N |
| interval | How often to attempt to consume `size` number of records. This only comes into play if the initial consume could not get a full slice. | 1000 | N |
|rollback_on_failure | Controls whether the consumer state is rolled back on failure. This will protect against data loss, however this can have an unintended side effect of blocking the job from moving if failures are minor and persistent. NOTE: This currently defaults to `false` due to the side effects of the behavior, at some point in the future it is expected this will default to `true`. | false | N |

# Job configuration example

This example reads from a topic `testing-topic` as part of the consumer group `testing-group` and outputs the result to stdout. It will wait 10 seconds per slice for up to 1000 records to be produced.

```
{
  "name": "Simple test",
  "lifecycle": "persistent",
  "workers": 1,
  "operations": [
    {
      "_op": "teraslice_kafka_reader",
      "size": 1000,
      "topic": "testing-topic",
      "group": "testing-group"
    },
    {
      "_op": "stdout"
    }
  ]
}
```

# Notes

 * This reader is primarily intended for persisent jobs. Better handling of once jobs may come in the future.
 * The reader will wait `wait` milliseconds for data to be produced before considering the slice complete. If no data shows up within that window then an empty slice will be produced. On a persistent job the next iteration will start the same process again and it will continue to process the queue in the same manner until the job is stopped.
