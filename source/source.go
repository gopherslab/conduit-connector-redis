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
package source

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-connector-redis/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	goredis "github.com/go-redis/redis/v8"
	redis "github.com/gomodule/redigo/redis"
)

type Source struct {
	sdk.UnimplementedSource

	config      config.Config
	client      redis.Conn
	redisClient *(goredis.Client)
	iterator    Iterator
}
type Iterator interface {
	HasNext(ctx context.Context) bool
	Next(ctx context.Context) (sdk.Record, error)
	Stop() error
}

func NewSource() sdk.Source {
	return &Source{}
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring a Source Connector...")
	config, err := config.Parse(cfg)
	if err != nil {
		return err
	}

	s.config = config
	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	if s.config.Mode == "pubsub" {
		address := s.config.Host + ":" + s.config.Port
		redisClient, err := redis.Dial("tcp", address)
		if err != nil {
			return fmt.Errorf("failed to connect redis client:%w", err)
		}
		s.client = redisClient
		redisClient2 := goredis.NewClient(&goredis.Options{
			Addr: fmt.Sprintf("%s:%s", s.config.Host, s.config.Port),
		})
		s.redisClient = redisClient2
		s.iterator, err = NewCDCIterator(ctx, s.client, s.config.Channel)
		if err != nil {
			return fmt.Errorf("couldn't create a iterator: %w", err)
		}
	} else {
		redisClient2 := goredis.NewClient(&goredis.Options{
			Addr: fmt.Sprintf("%s:%s", s.config.Host, s.config.Port),
		})
		s.redisClient = redisClient2
		var err error
		s.iterator, err = NewCDCIterator2(ctx, s.redisClient, s.config.Consumer, s.config.ConsumerGroup)
		if err != nil {
			return fmt.Errorf("couldn't create a iterator: %w", err)
		}
	}
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	if !s.iterator.HasNext(ctx) {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}
	data, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, err
	}
	return data, nil
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	if err := (s.client).Close(); err != nil {
		return fmt.Errorf("failed to close DB connection: %w", err)
	}

	if s.iterator != nil {
		err := s.iterator.Stop()
		if err != nil {
			return err
		}
		s.iterator = nil
	}
	return nil
}
