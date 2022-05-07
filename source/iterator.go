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
	"encoding/json"
	"fmt"
	"sync"

	sdk "github.com/conduitio/conduit-connector-sdk"
	goredis "github.com/go-redis/redis/v8"
	redis "github.com/gomodule/redigo/redis"
	"github.com/rs/xid"
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

func NewCDCIterator2(ctx context.Context, client *goredis.Client, consumer string, consumersGroup string) (*CDCIterator, error) {
	quit := make(chan bool, 1)
	cdc := &CDCIterator{
		channel: consumer,
		quit:    quit,
	}
	// consumer := "tickets"
	// consumersGroup := "tickets-consumer-group"
	err := client.XGroupCreate(ctx, consumer, consumersGroup, "0").Err()
	uniqueID := xid.New().String()
	if err != nil {
		return cdc, err
	}
	go func() {
		select {
		case <-cdc.quit:
			return
		default:
			for {
				entries, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
					Group:    consumersGroup,
					Consumer: uniqueID,
					Streams:  []string{consumer, ">"},
					Count:    2,
					Block:    0,
					NoAck:    false,
				}).Result()
				if err != nil {
					return
				}
				for i := 0; i < len(entries[0].Messages); i++ {
					messageID := entries[0].Messages[i].ID
					fmt.Println(messageID)
					values := entries[0].Messages[i].Values
					value, err := json.Marshal(values)
					if err != nil {
						return
					}
					data := sdk.Record{
						Payload: sdk.RawData([]byte(value)),
					}
					cdc.records = append(cdc.records, data)
					client.XAck(ctx, consumer, consumersGroup, messageID)
				}
			}
		}
	}()
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
