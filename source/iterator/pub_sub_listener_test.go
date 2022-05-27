/*
Copyright © 2022 Meroxa, Inc. & Gophers Lab Technologies Pvt. Ltd.

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
	"time"

	"github.com/alicebob/miniredis/v2"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gomodule/redigo/redis"
	"github.com/rafaeljusto/redigomock"
	"github.com/stretchr/testify/assert"
	"gopkg.in/tomb.v2"
)

func TestHasNext(t *testing.T) {
	tests := []struct {
		name     string
		records  []sdk.Record
		response bool
	}{
		{
			name:     "Has next",
			records:  []sdk.Record{{}},
			response: true,
		}, {
			name:     "no next value",
			records:  []sdk.Record{},
			response: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cdc = PubSubIterator{records: tt.records, tomb: &tomb.Tomb{}}
			res := cdc.HasNext()
			assert.Equal(t, res, tt.response, tt.name)
		})
	}
}
func TestStop(t *testing.T) {
	cdc := PubSubIterator{
		tomb: &tomb.Tomb{},
	}
	err := cdc.Stop()
	assert.Nil(t, err)
}

func TestNext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tmbWithCtx, ctx := tomb.WithContext(ctx)
	var cdc PubSubIterator
	cdc.tomb = tmbWithCtx
	dummyRec := sdk.Record{
		Position: nil,
		Metadata: nil,
		Key:      nil,
		Payload:  sdk.RawData("dummy_payload"),
	}
	cdc.mux = &sync.Mutex{}
	cdc.records = append(cdc.records, dummyRec)
	res, err := cdc.Next(ctx)
	assert.Nil(t, err)
	assert.Equal(t, res, dummyRec)
	res, err = cdc.Next(ctx)
	assert.Equal(t, sdk.Record{}, res)
	assert.EqualError(t, err, sdk.ErrBackoffRetry.Error())
	cancel()
	res, err = cdc.Next(ctx)
	assert.Equal(t, sdk.Record{}, res)
	assert.EqualError(t, err, "context canceled")
}

func TestNewCDCIterator(t *testing.T) {
	redisChannel := "subchannel"
	conn := redigomock.NewConn()
	response := PubSubIterator{
		key:     redisChannel,
		psc:     &redis.PubSubConn{Conn: conn},
		records: []sdk.Record{},
		mux:     &sync.Mutex{},
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
	res, err := NewPubSubIterator(context.Background(), conn, redisChannel)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, response.key, res.key)
	assert.Equal(t, response.psc, res.psc)
}

func TestNewCDCIterator_Next(t *testing.T) {
	redisChannel := "subchannel"
	testMessage := "some_dummy_message"
	testMessage1 := "some_dummy_message1"
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	conn, err := redis.Dial("tcp", mr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	response := PubSubIterator{
		key:     redisChannel,
		psc:     &redis.PubSubConn{Conn: conn},
		records: []sdk.Record{},
		mux:     &sync.Mutex{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := NewPubSubIterator(ctx, conn, redisChannel)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, response.key, res.key)
	assert.Equal(t, response.psc, res.psc)
	// publish is a fire and forget method, give a few ms for goroutines to start
	// otherwise messages might be lost and tests will fail
	time.Sleep(10 * time.Millisecond)
	mr.Publish(redisChannel, testMessage)
	mr.Publish(redisChannel, testMessage1)

	var rec sdk.Record
	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()
	retryCount := 0
	for {
		select {
		case <-ctx.Done():
		case <-ticker.C:
			rec, err = res.Next(ctx)
			if err != nil && err == sdk.ErrBackoffRetry {
				t.Log("backoff received, waiting for 400ms")
				if retryCount >= 10 {
					t.Error("retry count exceeded waiting for message, failing now")
					return
				}
				retryCount++
				continue
			}
			assert.NoError(t, err)
			assert.NotEmpty(t, rec.Payload)
			assert.Equal(t, string(rec.Payload.Bytes()), testMessage)

			cancel()
			// 2 messages were published, should not get empty record
			rec, err = res.Next(ctx)
			assert.NotEmpty(t, rec)
			assert.Equal(t, string(rec.Payload.Bytes()), testMessage1)
			assert.NoError(t, err)

			// no more messages, try Next again
			rec, err = res.Next(ctx)
			assert.Empty(t, rec)
			assert.EqualError(t, err, "context canceled")
			return
		}
	}
}
