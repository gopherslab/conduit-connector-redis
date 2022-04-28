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
	records []sdk.Record
	quit    chan bool
}

func NewCDCIterator(ctx context.Context, client redis.Conn, channel string) (*CDCIterator, error) {
	psc := redis.PubSubConn{Conn: client}
	var wg sync.WaitGroup
	wg.Add(1)
	cdc := &CDCIterator{
		client:  &client,
		channel: channel,
		psc:     psc,
		quit:    make(chan bool),
	}
	go func() {
		select {
		case <-cdc.quit:
			return
		default:

		}
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
				// c := map[string]sdk.Record{
				// 	n.Channel: data,
				// }
				cdc.records = append(cdc.records, data)
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
	val := i.records[0]
	remove(&i.records, 0)
	return val, nil
}
func (i *CDCIterator) Stop(ctx context.Context) error {
	i.quit <- true
	close(i.quit)
	return nil
}
func remove(a *[]sdk.Record, i int) *[]sdk.Record {
	copy((*a)[i:], (*a)[i+1:])
	*a = (*a)[:len((*a))-1]
	return a
}
