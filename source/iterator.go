package source

import (
	"context"
	"sync"

	sdk "github.com/conduitio/conduit-connector-sdk"
	redis "github.com/gomodule/redigo/redis"
)

type CDCIterator struct {
	client     *redis.Conn
	key        string
	psc        redis.PubSubConn
	testRecord map[string]sdk.Record
}

func NewCDCIterator(ctx context.Context, client redis.Conn, key string) (*CDCIterator, error) {
	psc := redis.PubSubConn{Conn: client}
	var wg sync.WaitGroup
	wg.Add(1)
	cdc := &CDCIterator{
		client:     &client,
		key:        key,
		psc:        psc,
		testRecord: make(map[string]sdk.Record),
	}
	go func() {
		for {
			switch n := psc.Receive().(type) {
			case redis.Message:
				data := sdk.Record{
					Payload: sdk.RawData(n.Data),
				}
				cdc.testRecord[cdc.key] = data
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
		psc.Subscribe(key)
	}()

	wg.Wait()
	return cdc, nil
}
func (i *CDCIterator) HasNext(ctx context.Context) bool {
	return len(i.testRecord) > 0
}

func (i *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	val := i.testRecord[i.key]
	delete(i.testRecord, i.key)
	return val, nil
}
