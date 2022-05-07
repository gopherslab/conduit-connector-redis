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
package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]string
		want   Config
		err    error
	}{
		{
			name: "Login with basic authentication",
			config: map[string]string{
				ConfigKeyHost:     "localhost",
				ConfigKeyPort:     "6379",
				ConfigKeyConsumer: "my_consumer",
				ConfigKeyDatabase: "0",
				ConfigKeyPassword: "12345678",
				ConfigKeyChannel:  "my_channel",
				ConfigKeyMode:     "pubsub",
			},
			want: Config{
				Host:     "localhost",
				Consumer: "my_consumer",
				Port:     "6379",
				Database: "0",
				Password: "12345678",
				Channel:  "my_channel",
				Mode:     "pubsub",
			},
			err: nil,
		},
		{
			name: "Invalid port",
			config: map[string]string{
				ConfigKeyHost:     "localhost",
				ConfigKeyConsumer: "my_consumer",
				ConfigKeyDatabase: "0",
				ConfigKeyPassword: "12345678",
				ConfigKeyChannel:  "my_channel",
				ConfigKeyMode:     "pubsub",
			},
			want: Config{},
			err:  fmt.Errorf("port config value must be set"),
		},
		{
			name: "Invalid host",
			config: map[string]string{

				ConfigKeyPort:     "6380",
				ConfigKeyConsumer: "my_consumer",
				ConfigKeyDatabase: "0",
				ConfigKeyPassword: "12345678",
				ConfigKeyChannel:  "my_channel",
			},
			want: Config{},
			err:  fmt.Errorf("host config value must be set"),
		},
		{
			name: "Invalid Mode",
			config: map[string]string{
				ConfigKeyHost:     "localhost",
				ConfigKeyPort:     "6380",
				ConfigKeyConsumer: "my_consumer",
				ConfigKeyPassword: "12345678",
				ConfigKeyChannel:  "my_channel",
				ConfigKeyMode:     "test",
			},
			want: Config{},
			err:  fmt.Errorf("mode contains unsupported value test, expected one of [pubsub stream]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.config)
			if tt.err != nil {
				assert.NotNil(t, err)
			} else {
				assert.NotNil(t, got)
				assert.Equal(t, got, tt.want)
			}
		})
	}
}
