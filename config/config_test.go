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

package config

import (
	"fmt"
	"testing"
	"time"

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
				KeyHost:     "localhost",
				KeyPort:     "6379",
				KeyRedisKey: "my_key",
				KeyPassword: "12345678",
				KeyMode:     "pubsub",
			},
			want: Config{
				Host:          "localhost",
				RedisKey:      "my_key",
				Port:          "6379",
				Database:      0,
				Password:      "12345678",
				Mode:          ModePubSub,
				PollingPeriod: time.Second,
			},
			err: nil,
		},
		{
			name: "Empty host and port passed",
			config: map[string]string{
				KeyRedisKey: "my_key",
				KeyDatabase: "1",
				KeyPassword: "12345678",
				KeyMode:     "stream",
			},
			want: Config{
				Host:          "localhost",
				RedisKey:      "my_key",
				Port:          "6379",
				Database:      1,
				Password:      "12345678",
				Mode:          ModeStream,
				PollingPeriod: time.Second,
			},
			err: nil,
		},
		{
			name: "Empty key passed",
			config: map[string]string{
				KeyPort:     "6380",
				KeyDatabase: "0",
				KeyPassword: "12345678",
			},
			want: Config{},
			err:  fmt.Errorf("host config value must be set"),
		},
		{
			name: "Invalid Mode",
			config: map[string]string{
				KeyHost:     "localhost",
				KeyPort:     "6380",
				KeyRedisKey: "my_key",
				KeyPassword: "12345678",
				KeyMode:     "test",
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
