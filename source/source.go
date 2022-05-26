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

//go:generate mockery --name=Iterator --outpkg mocks

package source

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-connector-redis/config"
	"github.com/conduitio/conduit-connector-redis/source/iterator"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gomodule/redigo/redis"
)

type Source struct {
	sdk.UnimplementedSource

	config   config.Config
	iterator Iterator
}

type Iterator interface {
	HasNext() bool
	Next(ctx context.Context) (sdk.Record, error)
	Stop() error
}

// NewSource returns an instance of sdk.Source
func NewSource() sdk.Source {
	return &Source{}
}

// Configure validates the passed config and prepares the source connector
func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Trace().Msg("Configuring a Source Connector...")
	conf, err := config.Parse(cfg)
	if err != nil {
		return fmt.Errorf("error parsing config: %w", err)
	}
	s.config = conf
	return nil
}

// Open prepare the plugin to start reading records from the given position
func (s *Source) Open(ctx context.Context, position sdk.Position) error {
	address := s.config.Host + ":" + s.config.Port
	dialOptions := make([]redis.DialOption, 0)
	if s.config.Password != "" {
		dialOptions = append(dialOptions, redis.DialPassword(s.config.Password))
	}
	if s.config.Username != "" {
		dialOptions = append(dialOptions, redis.DialUsername(s.config.Username))
	}
	dialOptions = append(dialOptions, redis.DialDatabase(s.config.Database))

	redisClient, err := redis.DialContext(ctx, "tcp", address, dialOptions...)
	if err != nil {
		return fmt.Errorf("failed to connect redis client: %w", err)
	}

	switch s.config.Mode {
	case config.ModePubSub:
		s.iterator, err = iterator.NewPubSubIterator(ctx, redisClient, s.config.RedisKey)
		if err != nil {
			return fmt.Errorf("couldn't create a pubsub iterator: %w", err)
		}
	case config.ModeStream:
		s.iterator, err = iterator.NewStreamIterator(ctx, redisClient, s.config.RedisKey, s.config.PollingPeriod, position)
		if err != nil {
			return fmt.Errorf("couldn't create a stream iterator: %w", err)
		}
	default:
		return fmt.Errorf("invalid mode(%v) encountered", s.config.Mode)
	}

	return nil
}

// Read gets the next object
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	if !s.iterator.HasNext() {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}
	rec, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("error fetching next record: %w", err)
	}
	return rec, nil
}

// Ack is called by the conduit server after the record has been successfully processed by all destination connectors
func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().
		Str("position", string(position)).
		Str("mode", string(s.config.Mode)).
		Msg("position ack received")
	return nil
}

// Teardown is called by the conduit server to stop the source connector
// all the cleanup should be done in this function
func (s *Source) Teardown(_ context.Context) error {
	if s.iterator != nil {
		err := s.iterator.Stop()
		if err != nil {
			return err
		}
		s.iterator = nil
	}
	return nil
}
