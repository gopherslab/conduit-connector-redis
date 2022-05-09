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
	"errors"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/conduitio/conduit-connector-redis/config"
	"github.com/conduitio/conduit-connector-redis/source/mocks"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/rafaeljusto/redigomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
	var cdc Source
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cdc.Configure(context.Background(), tt.field.cfg)
			if tt.errMsg {
				assert.NotNil(t, err)
			}
		})
	}
}

func TestNewSource(t *testing.T) {
	svc := NewSource()
	assert.NotNil(t, svc)
}

func TestOpen(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		source Source
	}{
		{
			name: "open stream",
			err:  nil,
			source: Source{
				config: config.Config{
					Mode: config.ModeStream,
				},
			},
		}, {
			name: "open errors",
			err:  errors.New("invalid mode(invalid_mode) encountered"),
			source: Source{
				config: config.Config{
					Mode: "invalid_mode",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr, err := miniredis.Run()
			assert.NoError(t, err)
			var s = tt.source
			s.config.Host = mr.Host()
			s.config.Port = mr.Port()
			err = s.Open(context.Background(), sdk.Position{})
			if tt.err != nil {
				assert.EqualError(t, err, tt.err.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestRead(t *testing.T) {
	tests := []struct {
		name     string
		response sdk.Record
		err      error
		source   Source
	}{
		{
			name:     "no records",
			response: sdk.Record{},
			err:      sdk.ErrBackoffRetry,
			source: Source{
				iterator: func() Iterator {
					m := &mocks.Iterator{}
					m.On("HasNext", mock.Anything).Return(false)
					return m
				}(),
			},
		},
		{
			name:     "records",
			response: sdk.Record{},
			err:      errors.New("mock error"),
			source: Source{
				iterator: func() Iterator {
					m := &mocks.Iterator{}
					m.On("HasNext", mock.Anything).Return(true)
					m.On("Next", mock.Anything).Return(sdk.Record{}, errors.New("mock error"))
					return m
				}(),
			},
		},
		{
			name:     "valid record",
			response: sdk.Record{},
			err:      nil,
			source: Source{
				iterator: func() Iterator {
					m := &mocks.Iterator{}
					m.On("HasNext", mock.Anything).Return(true)
					m.On("Next", mock.Anything).Return(sdk.Record{}, nil)
					return m
				}(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := tt.source.Read(context.Background())
			if tt.err != nil {
				assert.NotNil(t, err, res)
			} else {
				assert.NotNil(t, res)
				assert.Equal(t, res, tt.response)
			}
		})
	}
}

func TestTeardown(t *testing.T) {
	tests := []struct {
		name   string
		source Source
		err    error
	}{
		{
			name: "no client",
			source: Source{
				iterator: nil,
			},
			err: nil,
		},

		{
			name: " with iterator",
			source: Source{
				iterator: func() Iterator {
					m := &mocks.Iterator{}
					m.On("Stop", mock.Anything).Return(errors.New("mock error"))
					return m
				}(),
			},
			err: fmt.Errorf("mock error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.source.Teardown(context.Background())
			if tt.err != nil {
				assert.EqualError(t, err, tt.err.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestAck(t *testing.T) {
	var s Source
	test := struct {
		name string
		err  error
	}{
		name: "ack",
		err:  nil,
	}
	t.Run(test.name, func(t *testing.T) {
		err := s.Ack(context.Background(), sdk.Position{})
		if test.err != nil {
			assert.NotNil(t, test.err, err)
		} else {
			assert.Nil(t, err)
		}
	})
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
			d := new(Source)
			d.config.Mode = tt.mode
			d.config.Key = "dummy_key"
			err := d.validateKey(c)
			if tt.err != nil {
				assert.EqualError(t, err, tt.err.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
