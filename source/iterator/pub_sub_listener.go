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

package iterator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/tomb.v2"
)

type PubSubIterator struct {
	key     string
	psc     *redis.PubSubConn
	records []sdk.Record
	mux     *sync.Mutex
	tomb    *tomb.Tomb
}

// NewPubSubIterator creates a new instance of redis pubsub iterator and starts listening for new messages on channel
func NewPubSubIterator(ctx context.Context, client redis.Conn, key string) (*PubSubIterator, error) {
	psc := &redis.PubSubConn{Conn: client}
	tmbWithCtx, _ := tomb.WithContext(ctx)
	cdc := &PubSubIterator{
		// todo: explain somewhere in the docs that key in pubsub is a regex or a pattern
		key:     key,
		psc:     psc,
		mux:     &sync.Mutex{},
		tomb:    tmbWithCtx,
		records: make([]sdk.Record, 0),
	}

	// subscribe to all the channels matching the passed pattern
	if err := psc.Subscribe(key); err != nil {
		return nil, err
	}

	cdc.tomb.Go(cdc.startListener(ctx))

	return cdc, nil
}

// HasNext returns whether there are any more records to be returned
func (i *PubSubIterator) HasNext(_ context.Context) bool {
	return len(i.records) > 0 || !i.tomb.Alive()
}

// Next pops and returns the first message from records queue
func (i *PubSubIterator) Next(ctx context.Context) (sdk.Record, error) {
	// haris: why do we need this lock?
	i.mux.Lock()
	defer i.mux.Unlock()

	if len(i.records) > 0 {
		rec := i.records[0]
		i.records = i.records[1:]
		return rec, nil
	}
	select {
	case <-i.tomb.Dying():
		return sdk.Record{}, i.tomb.Err()
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	default:
		return sdk.Record{}, sdk.ErrBackoffRetry
	}
}

// Stop stops the listener and closes the connection to redis
// haris: it would be good to add a note about how the goroutines are handled
func (i *PubSubIterator) Stop() error {
	i.tomb.Kill(errors.New("listener stopped"))
	return nil
}

// startListener is the go routine function listening for new messages on provided channel in an infinite loop
func (i *PubSubIterator) startListener(ctx context.Context) func() error {
	return func() error {
		for {
			select {
			case <-i.tomb.Dying():
				if err := i.psc.Close(); err != nil {
					return fmt.Errorf("error closing the pubsub connection: %w", err)
				}
				return fmt.Errorf("tomb error: %w", i.tomb.Err())
			default:
				switch n := i.psc.Receive().(type) {
				case redis.Message:
					key := fmt.Sprintf("%s_%d", n.Channel, time.Now().UnixNano())
					// no position set, as redis doesn't persist the message, so message once lost can't be recovered
					data := sdk.Record{
						Metadata: map[string]string{
							"type": "message",
						},
						// a random position, to keep conduit server happy
						Position:  []byte(key),
						CreatedAt: time.Now(),
						Key:       sdk.RawData(key),
						Payload:   sdk.RawData(n.Data),
					}
					i.mux.Lock()
					i.records = append(i.records, data)
					i.mux.Unlock()
				case redis.Subscription:
					// haris: I'm thinking if we need it to be at info level...
					sdk.Logger(i.tomb.Context(ctx)).Info().
						Str("kind", n.Kind).
						Int("sub_count", n.Count).
						Str("channel", n.Channel).
						Msg("new subscription message received")
				case error:
					return n
					// haris: how important is it to handle that type?
					// todo: psc.Receive() docs show a type called "Pong", we don't have a case for it, or a default case
				}
			}
		}
	}
}
