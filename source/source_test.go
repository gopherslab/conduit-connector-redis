package source

import (
	"context"
	"errors"
	"testing"

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
	var s Source
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "open",
			err:  errors.New("failed to connect redis client"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.Open(context.Background(), sdk.Position{})
			if tt.err != nil {
				assert.NotNil(t, err)
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
	conn := redigomock.NewConn()
	tests := []struct {
		name   string
		source Source
		err    error
	}{
		{
			name: "no client",
			source: Source{
				client:   conn,
				iterator: nil,
			},
			err: nil,
		},
		{
			name: "client close",
			source: Source{
				client:   conn,
				iterator: nil,
			},
			err: nil,
		},

		{
			name: " with iterator",
			source: Source{
				client: conn,
				iterator: func() Iterator {
					m := &mocks.Iterator{}
					m.On("Stop", mock.Anything).Return(errors.New("mock error"))
					return m
				}(),
			},
			err: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.source.Teardown(context.Background())
			if tt.err != nil {
				assert.NotNil(t, tt.err, err)
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
