package source

import (
	"context"

	sdk "github.com/conduitio/conduit-connector-sdk"
	redis "github.com/gomodule/redigo/redis"
)

func NewCDCIterator(ctx context.Context, client redis.Conn, key string) (*sdk.Record, error) {
	psc := redis.PubSubConn{Conn: client}
	psc.PSubscribe("__keyspace@0__:" + key)

	ms := psc.Receive()
	switch msg := ms.(type) {
	case redis.Message:
		result := &sdk.Record{
			Metadata: map[string]string{
				"Channel": msg.Channel,
			},
			Payload: sdk.RawData(msg.Data),
		}
		return result, nil
	case redis.Subscription:
		result := &sdk.Record{
			Metadata: map[string]string{
				"Channel": msg.Channel,
			},
			Payload: sdk.RawData(msg.Channel),
		}
		if msg.Count == 0 {
			return result, nil
		}

	case error:
		return &sdk.Record{}, msg

	}
	return &sdk.Record{}, nil
}
