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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/tomb.v2"
)

type StreamIterator struct {
	key             string
	client          redis.Conn
	records         []sdk.Record
	mux             *sync.Mutex
	tomb            *tomb.Tomb
	lastID          string
	recordsPerCall  int
	pollingInterval time.Duration
	ticker          *time.Ticker
}

func NewStreamIterator(ctx context.Context, client redis.Conn, key string, pollingInterval time.Duration, position sdk.Position) (*StreamIterator, error) {
	tmbWithCtx, _ := tomb.WithContext(ctx)
	ticker := time.NewTicker(pollingInterval)

	lastID := string(position)
	if lastID == "" {
		// if position is empty, start from 0 record
		lastID = "0-0"
	}

	cdc := &StreamIterator{
		key:             key,
		client:          client,
		mux:             &sync.Mutex{},
		tomb:            tmbWithCtx,
		recordsPerCall:  1000, // move this to config?
		lastID:          lastID,
		pollingInterval: pollingInterval,
		ticker:          ticker,
	}

	cdc.tomb.Go(cdc.startIterator(ctx))

	return cdc, nil
}
func (i *StreamIterator) startIterator(ctx context.Context) func() error {
	return func() error {
		defer i.ticker.Stop()
		for {
			select {
			case <-i.tomb.Dying():
				if err := i.client.Close(); err != nil {
					return fmt.Errorf("error closing the redis client: %w", err)
				}
				return fmt.Errorf("tomb error: %w", i.tomb.Err())
			case <-i.ticker.C:
				resp, err := redis.Values(i.client.Do("XREAD", "COUNT", i.recordsPerCall, "STREAMS", i.key, i.lastID))
				if err != nil {
					if err == redis.ErrNil {
						sdk.Logger(ctx).Info().Str("key", i.key).Msg("no new data")
						continue
					}
					return fmt.Errorf("error reading data from stream: %w", err)
				}
				records, err := streamToRecords(resp)
				if err != nil {
					return fmt.Errorf("error converting stream data to records: %w", err)
				}
				if len(records) > 0 {
					i.mux.Lock()
					i.records = append(i.records, records...)
					i.lastID = string(records[len(records)-1].Position)
					i.mux.Unlock()
				}
			}
		}
	}
}

func (i *StreamIterator) HasNext(_ context.Context) bool {
	return len(i.records) > 0 || !i.tomb.Alive()
}

func (i *StreamIterator) Next(ctx context.Context) (sdk.Record, error) {
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
func (i *StreamIterator) Stop() error {
	i.tomb.Kill(errors.New("iterator stopped"))
	return nil
}

func streamToRecords(resp []interface{}) ([]sdk.Record, error) {
	records := make([]sdk.Record, 0)
	for _, iKey := range resp {
		var keyInfo = iKey.([]interface{})

		var key = keyInfo[0].([]byte)
		var idList = keyInfo[1].([]interface{})

		for _, iID := range idList {
			var idInfo = iID.([]interface{})

			position, ok := idInfo[0].([]byte)
			if !ok {
				return records, fmt.Errorf("error invalid id type received %T", idInfo[0])
			}
			posParts := strings.Split(string(position), "-")
			ts, err := strconv.ParseInt(posParts[0], 10, 64)
			if err != nil {
				return records, fmt.Errorf("invalid ts(%v): %w", posParts[0], err)
			}
			var fieldList = idInfo[1].([]interface{})
			rMap, err := arrInterfaceToMap(fieldList)
			if err != nil {
				return records, fmt.Errorf("error converting the []interface{} to map: %w", err)
			}

			payload, err := json.Marshal(rMap)
			if err != nil {
				return records, fmt.Errorf("error marshaling the map: %w", err)
			}
			records = append(records, sdk.Record{
				Position:  position,
				Metadata:  nil,
				CreatedAt: time.UnixMilli(ts),
				Key:       sdk.RawData(key),
				Payload:   sdk.RawData(payload),
			})
		}
	}
	return records, nil
}

func arrInterfaceToMap(values []interface{}) (map[string]string, error) {
	if len(values)%2 != 0 {
		return nil, fmt.Errorf("arrInterfaceToMap expects even number of values result, got %d", len(values))
	}

	m := make(map[string]string, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].([]byte)
		if !ok {
			return nil, fmt.Errorf("arrInterfaceToMap key[%d] not a bulk string value, got %T", i, values[i])
		}

		value, ok := values[i+1].([]byte)
		if !ok {
			return nil, fmt.Errorf("arrInterfaceToMap value[%d] not a bulk string value, got %T", i+1, values[i+1])
		}

		m[string(key)] = string(value)
	}
	return m, nil
}
