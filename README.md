# Conduit Connector Redis

### General

The Redis connector is one of [Conduit](https://github.com/ConduitIO/conduit) custom plugins. It provides both, a source
and a destination Redis connectors.

### How to build it

Run `make`.

### Testing

Run `make test` to run all the tests.

## Redis Source

The redis connector watches for new data being added in a *single* redis key supplied in `redis.key`. 
Currently, the connector supports two type of Redis Data structures(DS): `pubsub` & `stream`.
To decide which type of DS the redis key holds, the `mode` setting is used. 
The connector by default starts in `pubsub` mode and subscribes to the channel provided in `redis.key` settings using `SUBSCRIBE <redis.key>`
To start stream iterator pass `stream` as mode value.

**Q. Why can't we use `TYPE <key>` command to decide which iterator to start?**
A. There are 2 reasons for that:
- The pubsub channel names can conflict with existing keys of other DS types. 
For ex: there can be an existing key named `mystream` which holds stream DS and still the same name can be used as channel name
- `TYPE` command on channel name returns `none`


### Mode: pubsub

In this mode the source subscribes to the channel provided in `redis.key` setting, during configuration, and starts listening for new messages published on the channel.
The listener will stop only when the pipeline is paused or any error is encountered.
**NOTE:** The connector doesn't support pattern based channel subscription, it only subscribes to a single channel using `SUBSCRIBE <key>`
Whenever a new message is received, a new sdk.Record is created with received message as `payload`. The resulting sdk.Record has the following format:
```json
{
  "metadata": {
    "type": "message",
    "channel": "<channel>"
  },
  "position": "<channel>_<current_ns_timestamp>",
  "key": "<channel>",
  "payload": "<message received from channel>",
  "created_at": "<current_time in RFC3339 format>"
}
```
Where `position` value is an arbitrary position to satisfy the conduit server and same value is used in `key` to uniquely identify the messages

**Note:** The ([subscription messages](https://redis.io/docs/manual/pubsub/)) sent to the channel are not sent back to server, it is only logged as a trace level log.
Subscription messages are the messages confirming the successful subscription to the channel. 

### Mode: stream

While starting the iterator, the connector first checks the type of the key, the valid redis key is of type `none` (key doesn't exist) or `stream`,
Only then the iterator is initialized.
The stream iterator starts polling for new data every `pollingPeriod`. The new data slice is then inserted into a buffer that is checked on each Read request.
The resulting sdk.Record has the following format:
```json
{
  "metadata": {
    "key": "<key>"
  },
  "position": "<stream_msg_id>",
  "key": "<key>",
  "payload": "<JSON of key-val pair from stream>",
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

* Pub/Sub mode: The redis channel name is used as the record key

* Stream mode: The redis key name is used as the record key 


### Configuration

The config passed to `Configure` can contain the following fields.

| name             | description                                                                           | required | example            |
|------------------|---------------------------------------------------------------------------------------|----------|--------------------|
| `redis.key`      | the redis key to iterate over/subscribe(pattern subscription not supported)           | yes      | "mystream"         |
| `redis.host`     | Redis Host. default is "localhost"                                                    | no       | "localhost"        |
| `redis.port`     | Redis Port. default is "6379"                                                         | no       | "6379"             |
| `redis.database` | the redis database to use. default is "0"                                             | no       | "0"                |
| `redis.username` | the username to use for redis connection                                              | no       | "sample_user"      |
| `redis.password` | the password to use for redis connection                                              | no       | "sample_password"  |
| `mode`           | the mode of running the connector. default is pubsub                                  | no       | "pubsub", "stream" |
| `pollingPeriod`  | polling period for the CDC mode, formatted as a time.Duration string. default is "1s" | no       | "2s", "500ms"      |

### Known Limitations

* If a PUB/SUB message is lost due to system crash, it can not be retrieved back. Also, the messages published during the down-time will not be received.
* The connector doesn't support pattern/multiple channel subscription, it only subscribes to a single channel.
* The connector doesn't support multiple stream keys iteration, it only iterates over a single stream key.

### Planned for next phase
* Support pattern/multiple channel subscription
* Support multiple stream DS keys

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

| name             | description                                                                 | required | example            |
|------------------|-----------------------------------------------------------------------------|----------|--------------------|
| `redis.key`      | the redis key to iterate over/subscribe(pattern subscription not supported) | yes      | "mystream"         |
| `redis.host`     | Redis Host. default is "localhost"                                          | no       | "localhost"        |
| `redis.port`     | Redis Port. default is "6379"                                               | no       | "6379"             |
| `redis.database` | the redis database to use. default is "0"                                   | no       | "0"                |
| `redis.username` | the username to use for redis connection                                    | no       | "sample_user"      |
| `redis.password` | the password to use for redis connection                                    | no       | "sample_password"  |
| `mode`           | the mode of running the connector. default is pubsub                        | no       | "pubsub", "stream" |
