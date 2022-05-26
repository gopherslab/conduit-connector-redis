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

package destination

import (
	"context"
	"fmt"
	"testing"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/conduitio/conduit-connector-redis/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gomodule/redigo/redis"
	"github.com/rafaeljusto/redigomock"
	"github.com/stretchr/testify/assert"
)

func TestConfigure(t *testing.T) {
	invalidCfg := map[string]string{
		"host":     "localhost",
		"key":      "key",
		"port":     "6567",
		"database": "database",
		"password": "password",
		"channel":  "sample",
		"mode":     "test",
	}
	validConfig := map[string]string{
		"host":     "localhost",
		"key":      "key",
		"port":     "6567",
		"database": "database",
		"password": "password",
		"channel":  "sample",
		"mode":     "pubsub",
	}
	type field struct {
		cfg map[string]string
	}
	tests := []struct {
		name   string
		field  field
		want   config.Config
		errMsg bool
	}{
		{
			name: "valid config",
			field: field{
				cfg: validConfig,
			},
			errMsg: false,
		}, {
			name: "invalid config",
			field: field{
				cfg: invalidCfg,
			},
			errMsg: true,
		},
	}
	var destination Destination
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := destination.Configure(context.Background(), tt.field.cfg)
			if tt.errMsg {
				assert.NotNil(t, err)
			}
		})
	}
}

func TestNewDestination(t *testing.T) {
	svc := NewDestination()
	assert.NotNil(t, svc)
}

func TestOpenErr(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)

	d := new(Destination)
	d.config.Host = mr.Host()
	d.config.Port = mr.Port()
	d.config.Mode = "invalid_mode"
	d.config.Password = "dummy_password"
	mr.RequireAuth(d.config.Password)
	assert.EqualError(t, d.Open(context.Background()), "invalid mode(invalid_mode) encountered")
}

func TestOpenWithUserAuth(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)

	d := new(Destination)
	d.config.Host = mr.Host()
	d.config.Port = mr.Port()
	d.config.Mode = config.ModeStream
	d.config.Username = "dummy_user"
	d.config.Password = "dummy_password"
	mr.RequireUserAuth(d.config.Username, d.config.Password)
	assert.NoError(t, d.Open(context.Background()))
}

func TestOpen(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	d := new(Destination)
	d.config.Host = mr.Host()
	d.config.Port = mr.Port()
	d.config.Mode = config.ModeStream
	assert.NoError(t, d.Open(context.Background()))
}

func TestValidateKey(t *testing.T) {
	tests := []struct {
		name string
		mode config.Mode
		fn   func(conn *redigomock.Conn)
		err  error
	}{
		{
			name: "validate pubsub",
			mode: config.ModePubSub,
			fn:   func(conn *redigomock.Conn) {},
			err:  nil,
		}, {
			name: "validate stream, type none",
			mode: config.ModeStream,
			fn: func(conn *redigomock.Conn) {
				conn.Command("TYPE", "dummy_key").Expect("none")
			},
			err: nil,
		}, {
			name: "validate stream, type stream",
			mode: config.ModeStream,
			fn: func(conn *redigomock.Conn) {
				conn.Command("TYPE", "dummy_key").Expect("stream")
			},
			err: nil,
		}, {
			name: "validate stream fails",
			mode: config.ModeStream,
			fn: func(conn *redigomock.Conn) {
				conn.Command("TYPE", "dummy_key").Expect("string")
			},
			err: fmt.Errorf("invalid key type: string, expected none or stream"),
		}, {
			name: "invalid mode",
			mode: config.Mode("dummy_mode"),
			fn:   func(conn *redigomock.Conn) {},
			err:  fmt.Errorf("invalid mode(dummy_mode) encountered"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := redigomock.NewConn()
			tt.fn(c)
			d := new(Destination)
			d.config.Mode = tt.mode
			d.config.RedisKey = "dummy_key"
			err := d.validateKey(c)
			if tt.err != nil {
				assert.EqualError(t, err, tt.err.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestWrite(t *testing.T) {
	validJSON := []byte(`{"some":"json"}`)
	invalidJSON := []byte(`1,2,3,4`)
	key := "dummy_key"

	tests := []struct {
		name        string
		data        sdk.Record
		fn          func(conn *redigomock.Conn)
		err         error
		destination Destination
	}{
		{
			name: "invalid channel",
			data: sdk.Record{
				Payload: sdk.RawData(validJSON),
			},
			fn: func(conn *redigomock.Conn) {
				conn.GenericCommand("PUBLISH").ExpectError(fmt.Errorf("invalid channel"))
			},
			err: fmt.Errorf("error publishing message to channel(dummy_key): invalid channel"),
			destination: Destination{
				config: config.Config{
					Mode:     config.ModePubSub,
					RedisKey: key,
				},
			},
		}, {
			name: "pubsub success",
			data: sdk.Record{
				Payload: sdk.RawData(validJSON),
			},
			fn: func(conn *redigomock.Conn) {
				conn.GenericCommand("PUBLISH").Expect(1).ExpectError(nil)
			},
			err: nil,
			destination: Destination{
				config: config.Config{
					Mode:     config.ModePubSub,
					RedisKey: key,
				},
			},
		},
		{
			name: "stream success",
			data: sdk.Record{
				Payload: sdk.RawData(validJSON),
			},
			err: nil,
			fn: func(conn *redigomock.Conn) {
				conn.Command("XADD", key, "*", "some", "json").Expect("dummy_id")
			},
			destination: Destination{
				config: config.Config{
					Mode:     config.ModeStream,
					RedisKey: key,
				},
			},
		}, {
			name: "stream failed",
			data: sdk.Record{
				Payload: sdk.RawData(validJSON),
			},
			err: fmt.Errorf("error streaming message to key(dummy_key):dummy_error"),
			fn: func(conn *redigomock.Conn) {
				conn.Command("XADD", key, "*", "some", "json").ExpectError(fmt.Errorf("dummy_error"))
			},
			destination: Destination{
				config: config.Config{
					Mode:     config.ModeStream,
					RedisKey: key,
				},
			},
		},
		{
			name: "invalid mode",
			data: sdk.Record{
				Payload: sdk.RawData(invalidJSON),
			},
			err: fmt.Errorf("invalid mode(test) encountered"),
			destination: Destination{
				config: config.Config{
					Mode: "test",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := redigomock.NewConn()
			defer conn.Close()
			if tt.fn != nil {
				tt.fn(conn)
			}
			tt.destination.client = conn
			err := tt.destination.Write(context.Background(), tt.data)
			if tt.err != nil {
				assert.NotNil(t, err)
				assert.EqualError(t, err, tt.err.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestTeardown(t *testing.T) {
	tests := []struct {
		name   string
		client redis.Conn
		err    error
	}{
		{name: "client is nil", client: nil, err: nil},
		{name: "close succeeds", client: redigomock.NewConn(), err: nil},
		{name: "close fails", client: func() *redigomock.Conn {
			conn := redigomock.NewConn()
			conn.CloseMock = func() error {
				return fmt.Errorf("close failed")
			}
			return conn
		}(), err: fmt.Errorf("close failed")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Destination{client: tt.client}
			err := d.Teardown(context.Background())
			if tt.err == nil {
				assert.NoError(t, err)
				return
			}
			assert.EqualError(t, err, tt.err.Error())
		})
	}
}
