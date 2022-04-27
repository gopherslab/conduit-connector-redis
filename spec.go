package redis

import (
	"github.com/conduitio/conduit-connector-redis/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "Redis",
		Summary: "A Redis source and destination plugin for Conduit, written in Go.",
		Version: "v0.1.0",
		Author:  "gopherslab,Inc.",
		DestinationParams: map[string]sdk.Parameter{
			config.ConfigKeyHost: {
				Default:     "",
				Required:    true,
				Description: "host to the redis destination.",
			},
			config.ConfigKeyPort: {
				Default:     "",
				Required:    true,
				Description: "port to the redis destination.",
			},
			config.ConfigKeyDatabase: {
				Default:     "",
				Required:    true,
				Description: "database name for the redis destination.",
			},
		},
		SourceParams: map[string]sdk.Parameter{
			config.ConfigKeyHost: {
				Default:     "",
				Required:    true,
				Description: "host to the redis source.",
			},
			config.ConfigKeyPort: {
				Default:     "",
				Required:    true,
				Description: "port to the redis source",
			},
			config.ConfigKeyKey: {
				Default:     "",
				Required:    false,
				Description: "key name for connector to read.",
			},
			config.ConfigKeyDatabase: {
				Default:     "",
				Required:    false,
				Description: "database name for the redis source",
			},
			config.ConfigKeyPassword: {
				Default:     "",
				Required:    false,
				Description: "Password to the redis source.",
			},
			config.ConfigKeyChannel: {
				Default:     "",
				Required:    false,
				Description: "channel to the redis source listen.",
			},
			config.ConfigKeyMode: {
				Default:     "pubsub",
				Required:    false,
				Description: "Sets the connector's operation mode. Available modes: ['pubsub', 'stream']",
			},
		},
	}
}
