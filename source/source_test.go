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
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/conduitio/conduit-connector-redis/config"
	"github.com/conduitio/conduit-connector-redis/source/mocks"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestConfigure(t *testing.T) {
	validConfig := map[string]string{
		config.KeyRedisKey: "key",
		config.KeyMode:     string(config.ModeStream),
	}
	invalidCfg := map[string]string{
		config.KeyRedisKey: "key",
		config.KeyDatabase: "database",
		config.KeyMode:     string(config.ModePubSub),
	}
	type field struct {
		cfg map[string]string
	}
	tests := []struct {
		name   string
		field  field
		want   config.Config
		errMsg string
	}{
		{
			name: "valid config",
			field: field{
				cfg: validConfig,
			},
			errMsg: "",
		}, {
			name: "invalid config",
			field: field{
				cfg: invalidCfg,
			},
			errMsg: `error parsing config: invalid database passed, should be a valid int`,
		},
	}
	var cdc Source
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cdc.Configure(context.Background(), tt.field.cfg)
			if tt.errMsg != "" {
				assert.EqualError(t, err, tt.errMsg)
			} else {
				assert.NoError(t, err)
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
					Mode:          config.ModeStream,
					PollingPeriod: time.Second,
				},
			},
		}, {
			name: "open errors",
			err:  errors.New("invalid mode(invalid_mode) encountered"),
			source: Source{
				config: config.Config{
					Mode:          "invalid_mode",
					PollingPeriod: time.Second,
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err = s.Open(ctx, sdk.Position{})
			if tt.err != nil {
				assert.EqualError(t, err, tt.err.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestOpenWithUserAuth(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := new(Source)
	s.config.Host = mr.Host()
	s.config.Port = mr.Port()
	s.config.Mode = config.ModeStream
	s.config.Username = "dummy_user"
	s.config.Password = "dummy_password"
	s.config.PollingPeriod = time.Millisecond
	mr.RequireUserAuth(s.config.Username, s.config.Password)
	assert.NoError(t, s.Open(ctx, sdk.Position{}))
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
