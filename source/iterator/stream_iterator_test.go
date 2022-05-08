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

//
//  import (
// 	"context"
// 	"testing"
// 	"time"
//
// 	"github.com/rafaeljusto/redigomock"
// 	"github.com/stretchr/testify/assert"
// 	"gopkg.in/tomb.v2"
// )

//  func TestHasNextStream(t *testing.T) {
// 	var cdc = &StreamIterator{
// 		buffer: make(chan sdk.Record),
// 		tomb:   &tomb.Tomb{},
// 	}
// 	data := []byte("ABCD")
// 	record := sdk.Record{
// 		Payload: sdk.RawData(data),
// 	}
// 	go func() {
// 		cdc.buffer <- record
// 	}()
// 	tests := []struct {
// 		name     string
// 		response bool
// 	}{
// 		{
// 			name:     "Has next",
// 			response: true,
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			res := cdc.HasNext(context.Background())
// 			assert.Equal(t, tt.response, res, tt.name)
// 		})
// 	}
// }
//  func TestNextStream(t *testing.T) {
// 	var cdc StreamIterator
// 	data := []byte("ABCD")
// 	data2 := sdk.Record{
// 		Position: nil,
// 		Metadata: nil,
// 		Key:      nil,
// 		Payload:  sdk.RawData(data),
// 	}
// 	// cdc.records = append(cdc.records, data2)
// 	tests := []struct {
// 		name     string
// 		response sdk.Record
// 		err      error
// 	}{
// 		{
// 			name:     "test",
// 			response: data2,
// 			err:      nil,
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			res, err := cdc.Next(context.Background())
// 			if tt.err != nil {
// 				assert.NotNil(t, err)
// 			} else {
// 				assert.NotNil(t, res)
// 				assert.Equal(t, res, tt.response)
// 			}
// 		})
// 	}
// }
//  func TestStopStream(t *testing.T) {
// 	cdc := StreamIterator{
// 		tomb: &tomb.Tomb{},
// 	}
// 	test := struct {
// 		name string
// 		err  error
// 	}{
// 		name: "stop",
// 		err:  nil,
// 	}
// 	t.Run(test.name, func(t *testing.T) {
// 		err := cdc.Stop()
// 		if test.err != nil {
// 			assert.NotNil(t, test.err, err)
// 		} else {
// 			assert.Nil(t, err)
// 		}
// 	})
// }
// func TestNewStreamIterator(t *testing.T) {
// 	key := "tickets"
// 	pollingDuration, _ := time.ParseDuration("1s")
// 	conn := redigomock.NewConn()
// 	conn.Command("TYPE", key).Expect(string("stream"))
// 	cdc := &StreamIterator{
// 		key:             key,
// 		client:          conn,
// 		tomb:            &tomb.Tomb{},
// 		recordsPerCall:  1000,
// 		lastID:          "0-0",
// 		pollingInterval: pollingDuration,
// 		ticker:          time.NewTicker(pollingDuration),
// 	}
// 	tests := []struct {
// 		name     string
// 		response StreamIterator
// 		err      error
// 	}{
// 		{
// 			name:     "test",
// 			response: *cdc,
// 			err:      nil,
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			res, err := NewStreamIterator(context.Background(), conn, key, pollingDuration, nil)
// 			if tt.err != nil {
// 				assert.NotNil(t, err)
// 			} else {
// 				assert.NotNil(t, res, err)
// 			}
// 		})
// 	}
// }
// func TestStartIterator(t *testing.T) {
// 	key := "tickets"
// 	pollingDuration, _ := time.ParseDuration("10s")
// 	conn := redigomock.NewConn()
// 	conn.Command("XADD", key, "*", "sensor-id", "1234").Expect("1518951480106-0")
// 	cdc := &StreamIterator{
// 		key:             key,
// 		client:          conn,
// 		tomb:            &tomb.Tomb{},
// 		recordsPerCall:  1000,
// 		lastID:          "0-0",
// 		pollingInterval: pollingDuration,
// 		ticker:          time.NewTicker(pollingDuration),
// 	}
// 	tests := []struct {
// 		name string
// 		err  error
// 	}{
// 		{
// 			name: "test",
// 			err:  nil,
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			fn := cdc.startIterator(context.Background())
// 			err := fn()
// 			if tt.err != nil {
// 				assert.NotNil(t, err)
// 			}
// 		})
// 	}
// }
