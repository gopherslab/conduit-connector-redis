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
				ConfigKeyKey:      "my_key",
				ConfigKeyDatabase: "0",
				ConfigKeyPassword: "12345678",
				ConfigKeyChannel:  "my_channel",
				ConfigKeyMode:     "pubsub",
			},
			want: Config{
				Host:     "localhost",
				Key:      "my_key",
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
				ConfigKeyKey:      "my_key",
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
				ConfigKeyKey:      "my_key",
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
				ConfigKeyKey:      "my_key",
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
