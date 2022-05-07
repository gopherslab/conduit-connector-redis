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
package redis

import (
	"github.com/conduitio/conduit-connector-redis/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:              "Redis",
		Summary:           "A Redis source and destination plugin for Conduit, written in Go.",
		Version:           "v0.1.0",
		Author:            "gopherslab,Inc.",
		DestinationParams: map[string]sdk.Parameter{
			// config.ConfigKeyHost: {
			// 	Default:     "",
			// 	Required:    true,
			// 	Description: "host to the redis destination.",
			// },
			// config.ConfigKeyPort: {
			// 	Default:     "",
			// 	Required:    true,
			// 	Description: "port to the redis destination.",
			// },
			// config.ConfigKeyDatabase: {
			// 	Default:     "",
			// 	Required:    true,
			// 	Description: "database name for the redis destination.",
			// },
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
			config.ConfigKeyConsumer: {
				Default:     "",
				Required:    false,
				Description: "consumer name for connector to stream.",
			},
			config.ConfigKeyConsumerGroup: {
				Default:     "",
				Required:    false,
				Description: "consumergroup name for connector to stream.",
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
