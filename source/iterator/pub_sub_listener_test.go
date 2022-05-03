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
	"sync"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gomodule/redigo/redis"
	"github.com/rafaeljusto/redigomock"
	"github.com/stretchr/testify/assert"
	"gopkg.in/tomb.v2"
)

func TestHasNext(t *testing.T) {
	var cdc PubSubIterator
	data := []byte("ABCD")
	data2 := sdk.Record{
		Payload: sdk.RawData(data),
	}
	cdc.records = append(cdc.records, data2)
	tests := []struct {
		name     string
		response bool
	}{
		{
			name:     "Has next",
			response: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := cdc.HasNext(context.Background())
			assert.Equal(t, res, tt.response, tt.name)
		})
	}
}
func TestStop(t *testing.T) {
	cdc := PubSubIterator{
		tomb: &tomb.Tomb{},
	}
	test := struct {
		name string
		err  error
	}{
		name: "stop",
		err:  nil,
	}
	t.Run(test.name, func(t *testing.T) {
		err := cdc.Stop()
		if test.err != nil {
			assert.NotNil(t, test.err, err)
		} else {
			assert.Nil(t, err)
		}
	})
}
func TestNext(t *testing.T) {
	var cdc PubSubIterator
	data := []byte("ABCD")
	data2 := sdk.Record{
		Position: nil,
		Metadata: nil,
		Key:      nil,
		Payload:  sdk.RawData(data),
	}
	cdc.mux = &sync.Mutex{}
	cdc.records = append(cdc.records, data2)
	cdc.records = append(cdc.records, data2)
	tests := []struct {
		name     string
		response sdk.Record
		err      error
	}{
		{
			name:     "test",
			response: data2,
			err:      nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := cdc.Next(context.Background())
			if tt.err != nil {
				assert.NotNil(t, err)
			} else {
				assert.NotNil(t, res)
				assert.Equal(t, res, tt.response)
			}
		})
	}
}
func TestNewCDCIterator(t *testing.T) {
	redisChannel := "subchannel"
	conn := redigomock.NewConn()
	response := PubSubIterator{
		key:     redisChannel,
		psc:     &redis.PubSubConn{Conn: conn},
		records: []sdk.Record(nil),
	}

	conn.Command("SUBSCRIBE", redisChannel).Expect([]interface{}{
		[]byte("subscribe"),
		[]byte(redisChannel),
		[]byte("1"),
	})
	messages := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
		[]byte("finished"),
	}
	for _, message := range messages {
		conn.AddSubscriptionMessage([]interface{}{
			[]byte("message"),
			[]byte(redisChannel),
			message,
		})
	}
	tests := []struct {
		name     string
		response *PubSubIterator
		err      error
	}{
		{
			name:     "Success",
			response: &response,
			err:      nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := NewPubSubIterator(context.Background(), conn, redisChannel)
			if tt.err != nil {
				assert.NotNil(t, err)
			} else {
				assert.NotNil(t, res)
			}
		})
	}
}
