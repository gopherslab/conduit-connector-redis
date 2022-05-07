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
	"testing"

	"github.com/alicebob/miniredis"
	sdk "github.com/conduitio/conduit-connector-sdk"
	goredis "github.com/go-redis/redis/v8"
	redis "github.com/gomodule/redigo/redis"
	"github.com/rafaeljusto/redigomock"
	"github.com/stretchr/testify/assert"
)

func TestHasNext(t *testing.T) {
	var cdc CDCIterator
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
	cdc := CDCIterator{
		quit: make(chan bool, 1),
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
	var cdc CDCIterator
	data := []byte("ABCD")
	data2 := sdk.Record{
		Position: nil,
		Metadata: nil,
		Key:      nil,
		Payload:  sdk.RawData(data),
	}
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
	response := CDCIterator{
		channel: redisChannel,
		psc:     redis.PubSubConn{Conn: (conn)},
		records: []sdk.Record(nil),
		quit:    make(chan bool, 1),
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
		response *CDCIterator
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
			res, err := NewCDCIterator(context.Background(), conn, redisChannel)
			if tt.err != nil {
				assert.NotNil(t, err)
			} else {
				assert.NotNil(t, res)
			}
		})
	}
}
func TestNewCDCIterator2(t *testing.T) {
	consumer := "tickets"
	consumerGroup := "tickets-consumer-group"
	redisServer := mockRedis()
	redisClient := goredis.NewClient(&goredis.Options{
		Addr: redisServer.Addr(),
	})
	response := CDCIterator{
		channel: consumer,
		records: []sdk.Record(nil),
		quit:    make(chan bool, 1),
	}
	tests := []struct {
		name     string
		response *CDCIterator
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
			res, err := NewCDCIterator2(context.Background(), redisClient, consumer, consumerGroup)
			if tt.err != nil {
				assert.NotNil(t, err)
			} else {
				assert.NotNil(t, res)
			}
		})
	}
}
func mockRedis() *miniredis.Miniredis {
	s, err := miniredis.Run()

	if err != nil {
		panic(err)
	}

	return s
}
