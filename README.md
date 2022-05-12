# Conduit Connector Redis

### General

The Redis connector is one of [Conduit](https://github.com/ConduitIO/conduit) custom plugins. It provides both, a source
and a destination Redis connectors.

### How to build it

Run `make`.

### Testing

Run `make test` to run all the tests.

## Redis Source

The redis source has 2 modes

### PUBSUB

In this mode the source subscribes to the channel provided in key and starts listening for new messages published on the channel.
The listener will stop only when the pipeline is paused or any error is encountered.
Whenever a new message is received, a new sdk.Record is created with received message as `payload`. The resulting sdk.Record has the following format:
```json
{
  "metadata": {
    "type": "message"
  },
  "position": "<channel>_<current_ns_timestamp>",
  "key": "<channel>_<current_ns_timestamp>",
  "payload": "<message received from channel>",
  "created_at": "<current_time in RFC3339 format>"
}
```
Where `position` value is an arbitrary position to satisfy the conduit server and same value is used in `key` to uniquely identify the messages

**Note:** The messages sent to the channel (subscription messages) are not sent back to server, it is only logged as an info level log.

### STREAM

While starting the iterator, we first check the type of the key, if the key is of type `none` (key doesn't exist) or `stream`,
Only then the iterator is initialized.
The stream iterator starts polling for new data every `pollingPeriod`. The new data is then inserted into a buffer that is checked on each Read request.
The resulting sdk.Record has the following format:
```json
{
  "metadata": null,
  "position": "<stream_msg_id>",
  "key": "<stream_msg_id>",
  "payload": "<message received from channel>",
  "created_at": "<time>"
}
```

#### Position Handling

The connector goes through two modes.

* Pub/Sub mode: The pub/sub channels work in a fire and forget manner, so there is no scope of retrieving the message once lost.
So no position handling is required for pub-sub mode. 

* Stream mode: In stream mode, we iterate over the messages added in the stream using the message id as position. The message id of 
last successfully read message is used as the offset id for the subsequent XREAD requests.

### Record Keys

* Pub/Sub mode: An arbitrary key is created for Pub/Sub messages with the format: `<channel>_<current_ts_nano>`

* Stream mode: The message id assigned to the stream message on calling XADD is used to uniquely identify the records in case of Stream mode.


### Configuration

The config passed to `Configure` can contain the following fields.

| name             | description                                                                           | required | example                    |
|------------------|---------------------------------------------------------------------------------------|----------|----------------------------|
| `mode`           | the mode of running the connector                                                     | yes      | "PUBSUB"/"STREAM" |
| `redis.key`      | the redis key to iterate over/subscribe                                               | yes      | "mystream"                 |
| `redis.host`     | Redis Host. default is "localhost"                                                    | no       | "localhost"                |
| `redis.port`     | Redis Port. default is "6379"                                                         | no       | "6379"                     |
| `redis.database` | the redis database to use. default is "0"                                             | no       | "0"                        |
| `redis.password` | the password to use for redis connection                                              | no       | "sample_password"          |
| `pollingPeriod`  | polling period for the CDC mode, formatted as a time.Duration string. default is "1s" | no       | "2s", "500ms"              |

### Known Limitations

* If a PUB/SUB message is lost due to system crash, it can not be retrieved back. Also, the messages published during the down-time will not be received.

## Redis Destination

The Redis Destination Connector connects to a PUB/SUB channel or Redis stream key with the provided configurations, using the required
`key`, `mode` and other provided optional configs. Then will call `Configure` to parse the
configurations, If parsing was not successful, then an error will occur. After that, the `Open` method is called to
validate the key type. In case of Stream mode, the key should be either of type `none`(key doesn't exist) or `stream`.
In case of Pub/Sub mode there is no validation on channel type, as the channel name can coincide with existing keys of having different type. 

### Writer

The Redis destination implements only sync Write function, whenever a new message is received, it is pushed to redis key immediately.
In case of Stream Mode, the message should be of valid type `map[string]string`, an odd number of arguments will result in an error.

### Configuration

The config passed to `Configure` can contain the following fields.

| name             | description                                                                           | required | example                    |
|------------------|---------------------------------------------------------------------------------------|----------|----------------------------|
| `mode`           | the mode of running the connector                                                     | yes      | "PUBSUB"/"STREAM" |
| `redis.key`      | the redis key to iterate over/subscribe                                               | yes      | "mystream"                 |
| `redis.host`     | Redis Host. default is "localhost"                                                    | no       | "localhost"                |
| `redis.port`     | Redis Port. default is "6379"                                                         | no       | "6379"                     |
| `redis.database` | the redis database to use. default is "0"                                             | no       | "0"                        |
| `redis.password` | the password to use for redis connection                                              | no       | "sample_password"          |
