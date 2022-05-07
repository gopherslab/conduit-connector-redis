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
	"fmt"

	"github.com/conduitio/conduit-connector-redis/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gomodule/redigo/redis"
)

type Destination struct {
	sdk.UnimplementedDestination

	config config.Config
	client redis.Conn
}

func NewDestination() sdk.Destination {
	return &Destination{}
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring a Destination Connector...")
	conf, err := config.Parse(cfg)
	if err != nil {
		return fmt.Errorf("error parsing config: %w", err)
	}
	d.config = conf
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	address := d.config.Host + ":" + d.config.Port
	dialOptions := make([]redis.DialOption, 0)

	if d.config.Password != "" {
		dialOptions = append(dialOptions, redis.DialPassword(d.config.Password))
	}

	redisClient, err := redis.DialContext(ctx, "tcp", address, dialOptions...)
	if err != nil {
		return fmt.Errorf("failed to connect redis client: %w", err)
	}

	d.client = redisClient

	switch d.config.Mode {
	case config.ModePubSub:
		// no need to verify the type or if the channel exists
		// as we can create channel with a key even if that key already exists and have some other data type
	case config.ModeStream:
		keyType, err := redis.String(redisClient.Do("TYPE", d.config.Key))
		if err != nil {
			return fmt.Errorf("error fetching type of key(%s): %w", d.config.Key, err)
		}
		switch keyType {
		case "none", "stream":
		// valid key
		default:
			return fmt.Errorf("invalid key type: %s, expected none or stream", keyType)
		}
	default:
		return fmt.Errorf("invalid mode(%s) encountered", string(d.config.Mode))
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, rec sdk.Record) error {
	key := d.config.Key

	switch d.config.Mode {
	case config.ModePubSub:
		_, err := d.client.Do("PUBLISH", key, string(rec.Payload.Bytes()))
		if err != nil {
			return fmt.Errorf("error publishing message to channel(%s)", key)
		}
		return nil
	case config.ModeStream:

		keyValArgs, err := payloadToStreamArgs(rec.Payload)
		if err != nil {
			return fmt.Errorf("invalid payload: %w", err)
		}
		args := []interface{}{
			key, "*",
		}

		args = append(args, keyValArgs...)
		fmt.Println(keyValArgs)

		_, err = d.client.Do("XADD", args...)
		if err != nil {
			return fmt.Errorf("error streaming message to key(%s):%w", key, err)
		}
		return nil
	default:
		return fmt.Errorf("invalid mode(%s) encountered", string(d.config.Mode))
	}
}

func (d *Destination) Teardown(_ context.Context) error {
	return d.client.Close()
}

func payloadToStreamArgs(payload sdk.Data) ([]interface{}, error) {
	recMap := make(map[string]string)

	if err := json.Unmarshal(payload.Bytes(), &recMap); err != nil {
		fmt.Println(err)
		return nil, fmt.Errorf("invalid json received in payload: %w", err)
	}

	keyValArgs := make([]interface{}, 0, 2*len(recMap))
	for key, val := range recMap {
		keyValArgs = append(keyValArgs, key, val)
	}
	if len(keyValArgs) == 0 {
		return nil, fmt.Errorf("no key-value pair received")
	}
	return keyValArgs, nil
}