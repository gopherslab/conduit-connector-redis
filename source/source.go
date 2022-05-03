package source

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-connector-redis/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	redis "github.com/gomodule/redigo/redis"
)

type Source struct {
	sdk.UnimplementedSource

	config   config.Config
	client   redis.Conn
	iterator Iterator
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
	address := s.config.Host + ":" + s.config.Port
	redisClient, err := redis.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect redis client:%w", err)
	}
	s.client = redisClient
	s.iterator, err = NewCDCIterator(ctx, s.client, s.config.Channel)
	if err != nil {
		return fmt.Errorf("couldn't create a iterator: %w", err)
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
	if s.client != nil {
		if err := (s.client).Close(); err != nil {
			return fmt.Errorf("failed to close DB connection: %w", err)
		}
	}
	if s.iterator != nil {
		s.iterator.Stop()
		s.iterator = nil

	}
	return nil
}
