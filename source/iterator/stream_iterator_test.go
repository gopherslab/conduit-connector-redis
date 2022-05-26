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
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/rafaeljusto/redigomock"
	"github.com/stretchr/testify/assert"
	"gopkg.in/tomb.v2"
)

func TestNewStreamIterator(t *testing.T) {
	tests := []struct {
		name           string
		pos            sdk.Position
		fn             func(conn *redigomock.Conn)
		pollingPeriod  time.Duration
		err            error
		expectedLastID string
	}{
		{
			name:          "NewCDCIterator with lastModifiedTime=0",
			pos:           []byte(""),
			pollingPeriod: time.Second,
			fn: func(conn *redigomock.Conn) {
				conn.Command("TYPE", "dummy_key").Expect("none")
			},
			err:            nil,
			expectedLastID: "0-0",
		}, {
			name:          "NewCDCIterator with lastModifiedTime=2022-01-02T15:04:05Z",
			pos:           []byte("dummy_id"),
			pollingPeriod: time.Second,
			fn: func(conn *redigomock.Conn) {
				conn.Command("TYPE", "dummy_key").Expect("none")
			},
			err:            nil,
			expectedLastID: "dummy_id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "dummy_key"
			client := redigomock.NewConn()
			tt.fn(client)
			res, err := NewStreamIterator(context.Background(), client, key, tt.pollingPeriod, tt.pos)
			if tt.err != nil {
				assert.NotNil(t, err)
			} else {
				assert.NotNil(t, res)
				assert.NotNil(t, res.caches)
				assert.NotNil(t, res.buffer)
				assert.NotNil(t, res.tomb)
				assert.NotNil(t, res.ticker)
				assert.Equal(t, tt.pollingPeriod, res.pollingInterval)
				assert.Equal(t, client, res.client)
				assert.Equal(t, tt.expectedLastID, res.lastID)
				assert.True(t, res.tomb.Alive())
				res.tomb.Kill(fmt.Errorf("stop"))
			}
		})
	}
}

func TestStreamIterator_Next(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	tmbWithCtx, _ := tomb.WithContext(ctx)
	cdc := &StreamIterator{
		buffer: make(chan sdk.Record, 1),
		caches: make(chan []sdk.Record, 1),
		tomb:   tmbWithCtx,
	}
	cdc.tomb.Go(cdc.flush)

	in := sdk.Record{Position: []byte("some_position")}
	cdc.caches <- []sdk.Record{in}
	out, err := cdc.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, in, out)
	cancel()
	out, err = cdc.Next(ctx)
	assert.EqualError(t, err, ctx.Err().Error())
	assert.Empty(t, out)
}

func TestStreamIterator_HasNext(t *testing.T) {
	tests := []struct {
		name     string
		fn       func(c *StreamIterator)
		response bool
	}{{
		name: "Has next",
		fn: func(c *StreamIterator) {
			c.buffer <- sdk.Record{}
		},
		response: true,
	}, {
		name:     "no record in buffer",
		fn:       func(c *StreamIterator) {},
		response: false,
	}, {
		name: "record in buffer, tomb dead",
		fn: func(c *StreamIterator) {
			c.tomb.Kill(errors.New("random error"))
			c.buffer <- sdk.Record{}
		},
		response: true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cdc := &StreamIterator{buffer: make(chan sdk.Record, 1), tomb: &tomb.Tomb{}}
			tt.fn(cdc)
			res := cdc.HasNext()
			assert.Equal(t, res, tt.response)
		})
	}
}

func TestFlush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	tmbWithCtx, _ := tomb.WithContext(ctx)
	cdc := &StreamIterator{
		buffer: make(chan sdk.Record, 1),
		caches: make(chan []sdk.Record, 1),
		tomb:   tmbWithCtx,
	}
	randomErr := errors.New("random error")
	cdc.tomb.Go(cdc.flush)

	in := sdk.Record{Position: []byte("some_position")}
	cdc.caches <- []sdk.Record{in}
	for {
		select {
		case <-cdc.tomb.Dying():
			assert.EqualError(t, cdc.tomb.Err(), randomErr.Error())
			cancel()
			return
		case out := <-cdc.buffer:
			assert.Equal(t, in, out)
			cdc.tomb.Kill(randomErr)
		}
	}
}

func TestStartIterator(t *testing.T) {
	key := "dummy_key"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn := redigomock.NewConn()
	tmbWithCtx, _ := tomb.WithContext(ctx)
	conn.Command("XREAD", "COUNT", 10, "STREAMS", key, "0-0").Expect([]interface{}{[]interface{}{[]byte(key), []interface{}{[]interface{}{[]byte("1652107432000-0"), []interface{}{[]byte("key"), []byte("value")}}}}})
	cdc := &StreamIterator{key: key, caches: make(chan []sdk.Record, 1), ticker: time.NewTicker(time.Millisecond), recordsPerCall: 10, lastID: "0-0", tomb: tmbWithCtx, client: conn}
	_ = cdc.startIterator(ctx)()
	select {
	case cache := <-cdc.caches:
		cancel()
		assert.Len(t, cache, 1)
		assert.Equal(t, []sdk.Record{{
			Position: []byte("1652107432000-0"),
			Metadata: map[string]string{
				"key": key,
			},
			CreatedAt: time.UnixMilli(1652107432000),
			Key:       sdk.RawData(key),
			Payload:   sdk.RawData(`{"key":"value"}`),
		}}, cache)
	case <-ctx.Done():
		t.Error("no data received in cache channel")
	}
}

func TestStartIterator_Err(t *testing.T) {
	key := "dummy_key"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn := redigomock.NewConn()
	tmbWithCtx, _ := tomb.WithContext(ctx)
	conn.Command("XREAD", "COUNT", 10, "STREAMS", key, "0-0").Expect([]interface{}{[]interface{}{[]byte(key), []interface{}{[]interface{}{[]byte("1652107432000-0"), []interface{}{[]byte("key")}}}}})
	cdc := &StreamIterator{key: key, caches: make(chan []sdk.Record, 1), ticker: time.NewTicker(time.Millisecond), recordsPerCall: 10, lastID: "0-0", tomb: tmbWithCtx, client: conn}
	err := cdc.startIterator(ctx)()
	assert.EqualError(t, err, "error converting stream data to records: error converting the []interface{} to map: arrInterfaceToMap expects even number of values result, got 1")
}

func TestStreamIterator_Stop(t *testing.T) {
	cdc := StreamIterator{
		tomb:   &tomb.Tomb{},
		ticker: time.NewTicker(time.Second),
		client: redigomock.NewConn(),
	}
	err := cdc.Stop()
	assert.Nil(t, err)
	assert.Nil(t, cdc.client)
	assert.False(t, cdc.tomb.Alive())
}
