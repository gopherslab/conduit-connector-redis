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
// or when the error is to be returned by the Next function
func (i *PubSubIterator) HasNext() bool {
	return len(i.records) > 0 || !i.tomb.Alive() // if tomb is dead we return true so caller will fetch error with Next
}

// Next pops and returns the first message from records queue
func (i *PubSubIterator) Next(ctx context.Context) (sdk.Record, error) {
	// acquire lock before popping out the first record from records slice
	// this is to avoid simultaneous write from startIterator goroutine
	i.mux.Lock()
	defer i.mux.Unlock()

	if len(i.records) > 0 {
		// pop the first record from the records slice
		rec := i.records[0]
		i.records = i.records[1:] // remove the first record from slice
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

// Stop sends a kill signal to tomb, converting the tomb status to Dying
// giving go routines time to gracefully stop execution
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
							"type":    "message",
							"channel": n.Channel,
						},
						// a random position, to keep conduit server happy
						Position:  []byte(key),
						CreatedAt: time.Now(),
						Key:       sdk.RawData(n.Channel),
						Payload:   sdk.RawData(n.Data),
					}

					// acquire lock before appending the new records to records slice, to avoid race between Next() and append
					i.mux.Lock()
					i.records = append(i.records, data)
					i.mux.Unlock()
				case redis.Subscription:
					// this message is only received at time of successful subscription/unsubscription
					sdk.Logger(i.tomb.Context(ctx)).Trace().
						Str("kind", n.Kind).
						Int("sub_count", n.Count).
						Str("channel", n.Channel).
						Msg("new subscription message received")
				case redis.Pong:
					sdk.Logger(i.tomb.Context(ctx)).Trace().
						Msg("pong message received")
				case error:
					return n
				default:
					// There can only be 4 type of messages, if in future a new message type is added log unknown type error
					// So we can rectify it ASAP, instead of it being buried in lower level logs
					sdk.Logger(ctx).Error().Str("msg_type", fmt.Sprintf("%T", n)).Msg("unknown message type received")
					// skip returning error, so as not to break existing functionality
				}
			}
		}
	}
}
