package source

import (
	"context"
	"sync"

	sdk "github.com/conduitio/conduit-connector-sdk"
	redis "github.com/gomodule/redigo/redis"
)

type CDCIterator struct {
	client     *redis.Conn
	channel    string
	psc        redis.PubSubConn
	testRecord []map[string]sdk.Record
}

func NewCDCIterator(ctx context.Context, client redis.Conn, channel string) (*CDCIterator, error) {
	psc := redis.PubSubConn{Conn: client}
	var wg sync.WaitGroup
	wg.Add(1)
	cdc := &CDCIterator{
		client:  &client,
		channel: channel,
		psc:     psc,
	}
	go func() {
		for {
			switch n := psc.Receive().(type) {
			case redis.Message:
				data := sdk.Record{
					Payload: sdk.RawData(n.Data),
				}
				sdk.Logger(ctx).Info().
					Str("channel", n.Channel).
					Str("data", string(n.Data)).
					Msg("message")
				c := map[string]sdk.Record{
					n.Channel: data,
				}
				cdc.testRecord = append(cdc.testRecord, c)
			case redis.Subscription:
				if n.Count == 0 {
					return
				}
			case error:
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		psc.Subscribe(channel)
	}()

	wg.Wait()
	return cdc, nil
}
func (i *CDCIterator) HasNext(ctx context.Context) bool {
	return len(i.testRecord) > 0
}

func (i *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	val := i.testRecord[0][i.channel]
	remove(&i.testRecord, 0)
	return val, nil
}
func (i *CDCIterator) Stop(ctx context.Context) {

}
func remove(a *[]map[string]sdk.Record, i int) *[]map[string]sdk.Record {
	copy((*a)[i:], (*a)[i+1:])
	*a = (*a)[:len((*a))-1] // Truncate slice.
	return a
}
