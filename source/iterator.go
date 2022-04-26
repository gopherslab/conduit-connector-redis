package source

import (
	"context"
	"sync"

	sdk "github.com/conduitio/conduit-connector-sdk"
	redis "github.com/gomodule/redigo/redis"
)

type CDCIterator struct {
	client  *redis.Conn
	channel string
	psc     redis.PubSubConn
	records map[string]sdk.Record
}

func NewCDCIterator(ctx context.Context, client redis.Conn, channel string) (*CDCIterator, error) {
	psc := redis.PubSubConn{Conn: client}
	var wg sync.WaitGroup
	wg.Add(1)
	cdc := &CDCIterator{
		client:  &client,
		channel: channel,
		psc:     psc,
		records: make(map[string]sdk.Record),
	}
	go func() {
		for {
			switch n := psc.Receive().(type) {
			case redis.Message:
				data := sdk.Record{
					Payload: sdk.RawData(n.Data),
				}
				cdc.records[cdc.channel] = data
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
	return len(i.records) > 0
}

func (i *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	val := i.records[i.channel]
	delete(i.records, i.channel)
	return val, nil
}
func (i *CDCIterator) Stop(ctx context.Context) {

}
