/*
Copyright © 2022 Meroxa, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package source

import (
	"context"
	"sync"

	sdk "github.com/conduitio/conduit-connector-sdk"
	redis "github.com/gomodule/redigo/redis"
)

type CDCIterator struct {
	channel string
	psc     redis.PubSubConn
	records []sdk.Record
	quit    chan bool
}

func NewCDCIterator(ctx context.Context, client redis.Conn, channel string) (*CDCIterator, error) {
	psc := redis.PubSubConn{Conn: client}
	var wg sync.WaitGroup
	quit := make(chan bool, 1)
	wg.Add(1)
	cdc := &CDCIterator{
		channel: channel,
		psc:     psc,
		quit:    quit,
	}
	go func() {
		select {
		case <-cdc.quit:
			return
		default:
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
					cdc.records = append(cdc.records, data)
				case redis.Subscription:
					if n.Count == 0 {
						return
					}
				case error:
					return
				}
			}
		}
	}()
	errs := make(chan error, 1)
	go func() {
		defer wg.Done()
		err := psc.Subscribe(channel)
		if err != nil {
			errs <- err
		}
		close(errs)
	}()

	wg.Wait()
	return cdc, <-errs
}
func (i *CDCIterator) HasNext(ctx context.Context) bool {
	return len(i.records) > 0
}

func (i *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	val := i.records[0]
	remove(&i.records, 0)
	return val, nil
}
func (i *CDCIterator) Stop() error {
	i.quit <- true
	close(i.quit)
	return nil
}
func remove(a *[]sdk.Record, i int) *[]sdk.Record {
	copy((*a)[i:], (*a)[i+1:])
	*a = (*a)[:len((*a))-1]
	return a
}
