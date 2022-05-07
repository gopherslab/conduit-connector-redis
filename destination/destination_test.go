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
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/conduitio/conduit-connector-redis/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
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

func TestOpen(t *testing.T) {
	var d Destination
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
			err := d.Open(context.Background())
			if tt.err != nil {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestWrite(t *testing.T) {
	conn := redigomock.NewConn()
	jsonString := `{"some":"json"}`

	invalidData := []byte(jsonString)
	data := map[string]string{
		"key": jsonString,
	}
	validData, _ := json.Marshal(data)

	tests := []struct {
		name        string
		data        sdk.Record
		err         error
		destination Destination
	}{
		{
			name: "invalid channel",
			data: sdk.Record{
				Payload: sdk.RawData(validData),
			},
			err: fmt.Errorf("error publishing message to channel()"),
			destination: Destination{
				config: config.Config{
					Mode: "pubsub",
				},
				client: conn,
			},
		},
		{
			name: " stream data",
			data: sdk.Record{
				Payload: sdk.RawData(validData),
			},
			err: fmt.Errorf("invalid payload"),
			destination: Destination{
				config: config.Config{
					Mode: "stream",
				},
				client: conn,
			},
		},
		{
			name: "invalid stream",
			data: sdk.Record{
				Payload: sdk.RawData(invalidData),
			},
			err: fmt.Errorf("invalid payload: invalid json received in payload: invalid character 'A' looking for beginning of value"),
			destination: Destination{
				config: config.Config{
					Mode: "stream",
				},
				client: conn,
			},
		},
		{
			name: "invalid mode",
			data: sdk.Record{
				Payload: sdk.RawData(validData),
			},
			err: fmt.Errorf("invalid mode(test) encountered"),
			destination: Destination{
				config: config.Config{
					Mode: "test",
				},
				client: conn,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.destination.Write(context.Background(), tt.data)
			if tt.err != nil {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
