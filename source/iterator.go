package source

import (
	"context"
	"fmt"
	"sync"

	sdk "github.com/conduitio/conduit-connector-sdk"
	redis "github.com/gomodule/redigo/redis"
)

type CDCIterator struct {
	client  *redis.Conn
	records []sdk.Record
	key     string
}

func NewCDCIterator(ctx context.Context, client redis.Conn, key string) (*CDCIterator, error) {
	psc := redis.PubSubConn{Conn: client}
	var wg sync.WaitGroup
	wg.Add(2)

	cdc := &CDCIterator{
		client: &client,
		key:    key,
	}
	sdk.Logger(ctx).Info().Msg("This is NewCDCIterator function")
	go func() {
		defer wg.Done()
		for {
			switch n := psc.Receive().(type) {
			case redis.Message:
				record := map[string]string{
					n.Channel: string(n.Data),
				}
				data := sdk.Record{
					Payload: sdk.RawData(n.Data),
				}
				cdc.records = append(cdc.records, data)
				fmt.Println(record)
			case redis.Subscription:
				fmt.Printf("Subscription: %s %s %d\n", n.Kind, n.Channel, n.Count)
				if n.Count == 0 {
					fmt.Println("count zero")
					// return
				}
			case error:
				fmt.Printf("error: %v\n", n)
				// return
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
	return len(i.records) > 0
}

func (i *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	for val, key := range i.records {
		fmt.Println(val)
		fmt.Println(key)
	}
	return sdk.Record{}, ctx.Err()
}
