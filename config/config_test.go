package config

import (
	"fmt"
	"reflect"
	"testing"
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
			},
			want: Config{
				Host:     "localhost",
				Key:      "my_key",
				Port:     "6379",
				Database: "0",
				Password: "12345678",
				Channel:  "my_channel",
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
			name: "Invalid database",
			config: map[string]string{
				ConfigKeyHost: "localhost",
				ConfigKeyPort: "6380",
				ConfigKeyKey:  "my_key",

				ConfigKeyPassword: "12345678",
				ConfigKeyChannel:  "my_channel",
			},
			want: Config{},
			err:  fmt.Errorf("database config value must be set"),
		},
		{
			name: "Invalid database",
			config: map[string]string{
				ConfigKeyHost:     "localhost",
				ConfigKeyPort:     "6380",
				ConfigKeyDatabase: "0",
				ConfigKeyPassword: "12345678",
				ConfigKeyChannel:  "my_channel",
			},
			want: Config{},
			err:  fmt.Errorf("key config value must be set"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := Parse(tt.config); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse = %v, want = %v", got, tt.want)
			}
		})
	}
}
