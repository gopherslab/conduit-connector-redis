package source

import (
	"context"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	redis "github.com/gomodule/redigo/redis"
	"gopkg.in/tomb.v2"
)

type CDCIterator struct {
	client  *redis.Conn
	message redis.Message
	subs    redis.Subscription
	records map[string]string
	tomb    *tomb.Tomb
	key     string
}

func NewCDCIterator(ctx context.Context, client redis.Conn, key string) (*CDCIterator, error) {
	cdc := &CDCIterator{
		client:  &client,
		records: make(map[string]string),
		tomb:    &tomb.Tomb{},
		key:     key,
	}
	cdc.tomb.Go(cdc.getRedisData)
	return cdc, nil
}
func (i *CDCIterator) HasNext(ctx context.Context) bool {
	return len(i.records) > 0
}

func (i *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	result := &sdk.Record{
		Metadata: map[string]string{
			"Channel": i.message.Channel,
		},
		Payload: sdk.RawData(i.message.Channel),
	}

	return *result, nil
}

func (i *CDCIterator) getRedisData() error {
	psc := redis.PubSubConn{Conn: *i.client}
	psc.PSubscribe("__keyspace@0__:" + i.key)

	ms := psc.Receive()
	for {
		switch msg := ms.(type) {
		case redis.Message:
			// test := &CDCIterator{
			// 	client:  &client,
			// 	message: msg,
			// }
			fmt.Println("message", msg.Data)

			// result := &sdk.Record{
			// 	Metadata: map[string]string{
			// 		"Channel": msg.Channel,
			// 	},
			// 	Payload: sdk.RawData(msg.Data),
			// }
			// return test, nil
		case redis.Subscription:
			fmt.Println("subscription", msg.Channel)
			// result := &sdk.Record{
			// 	Metadata: map[string]string{
			// 		"Channel": msg.Channel,
			// 	},
			// 	Payload: sdk.RawData(msg.Channel),
			// }
			// if msg.Count == 0 {
			// 	return result, nil
			// }
			// test := &CDCIterator{
			// 	client: &client,
			// 	subs:   msg,
			// }
			// return test, nil

		case error:
			// return &CDCIterator{}, msg

		}
	}

	return i.tomb.Err()
}
