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
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/tomb.v2"
)

type StreamIterator struct {
	key             string
	client          redis.Conn
	tomb            *tomb.Tomb
	lastID          string
	recordsPerCall  int
	pollingInterval time.Duration
	ticker          *time.Ticker
	cache           chan []sdk.Record
	buffer          chan sdk.Record
}

func NewStreamIterator(ctx context.Context,
	client redis.Conn,
	key string,
	pollingInterval time.Duration,
	position sdk.Position) (*StreamIterator, error) {
	// validate if key either doesn't exist or is of type other than stream
	keyType, err := redis.String(client.Do("TYPE", key))
	if err != nil {
		return nil, fmt.Errorf("error fetching type of key(%s): %w", key, err)
	}
	switch keyType {
	case "none", "stream":
	// valid key
	default:
		return nil, fmt.Errorf("invalid key type: %s, expected none or stream", keyType)
	}

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
		tomb:            tmbWithCtx,
		recordsPerCall:  1000, // move this to config?
		lastID:          lastID,
		pollingInterval: pollingInterval,
		ticker:          ticker,
		cache:           make(chan []sdk.Record),
		buffer:          make(chan sdk.Record, 1),
	}

	cdc.tomb.Go(cdc.startIterator(ctx))
	cdc.tomb.Go(cdc.flush)

	return cdc, nil
}

func (i *StreamIterator) HasNext(_ context.Context) bool {
	return len(i.buffer) > 0 || !i.tomb.Alive() // if tomb is dead we return true so caller will fetch error with Next
}

func (i *StreamIterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case rec := <-i.buffer:
		return rec, nil
	case <-i.tomb.Dying():
		return sdk.Record{}, i.tomb.Err()
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	}
}
func (i *StreamIterator) Stop() error {
	i.ticker.Stop()
	i.tomb.Kill(errors.New("iterator stopped"))
	if err := i.client.Close(); err != nil {
		return fmt.Errorf("error closing the redis client: %w", err)
	}
	return nil
}

func (i *StreamIterator) startIterator(ctx context.Context) func() error {
	return func() error {
		defer close(i.cache)
		for {
			select {
			case <-i.tomb.Dying():
				return i.tomb.Err()
			case <-i.ticker.C:
				resp, err := redis.Values(i.client.Do("XREAD", "COUNT", i.recordsPerCall, "STREAMS", i.key, i.lastID))
				if err != nil {
					if err == redis.ErrNil {
						continue
					}
					return fmt.Errorf("error reading data from stream: %w", err)
				}
				records, err := toRecords(resp)
				if err != nil {
					return fmt.Errorf("error converting stream data to records: %w", err)
				}

				// ensure we don't fetch and keep a lot of records in memory
				// block till flush reads current array of records
				select {
				case i.cache <- records:
					i.lastID = string(records[len(records)-1].Position)

				case <-i.tomb.Dying():
					return i.tomb.Err()
				}
			}
		}
	}
}

func (i *StreamIterator) flush() error {
	defer close(i.buffer)
	for {
		select {
		case <-i.tomb.Dying():
			return i.tomb.Err()
		case cache := <-i.cache:
			for _, record := range cache {
				i.buffer <- record
			}
		}
	}
}

func toRecords(resp []interface{}) ([]sdk.Record, error) {
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
