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
	"context"
	"errors"
	"fmt"

	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/conduitio/conduit-connector-redis/config"
	"github.com/conduitio/conduit-connector-redis/destination"
	"github.com/conduitio/conduit-connector-redis/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.uber.org/goleak"
)

const (
	sourceKey = "dummy_key_source"
	destKey   = "dummy_key_dest"
)

func TestAcceptance(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	sourceConf := map[string]string{
		config.KeyHost:          mr.Host(),
		config.KeyPort:          mr.Port(),
		config.KeyRedisKey:      sourceKey,
		config.KeyMode:          string(config.ModeStream),
		config.KeyPollingPeriod: time.Millisecond.String(),
	}
	destConf := map[string]string{
		config.KeyHost:     mr.Host(),
		config.KeyPort:     mr.Port(),
		config.KeyRedisKey: destKey,
		config.KeyMode:     string(config.ModeStream),
	}

	sdk.AcceptanceTest(t, AcceptanceTestDriver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{Config: sdk.ConfigurableAcceptanceTestDriverConfig{
			Connector: sdk.Connector{
				NewSpecification: Specification,
				NewSource:        source.NewSource,
				NewDestination:   destination.NewDestination,
			},
			SourceConfig:      sourceConf,
			DestinationConfig: destConf,
			GoleakOptions:     []goleak.Option{goleak.IgnoreCurrent()},
			Skip: []string{
				// It calls Write & WriteAsync function directly, with non structure data generated using random strings
				// and there is no option to substitute existing generateRecords function
				"TestDestination_Write_Success",
			},
			AfterTest: func(t *testing.T) {
				mr.FlushAll()
			},
		},
		},
		rand: rand.New(rand.NewSource(time.Now().UnixNano())), // nolint: gosec // only used for testing purpose
	})
}

type AcceptanceTestDriver struct {
	rand *rand.Rand
	sdk.ConfigurableAcceptanceTestDriver
}

func (d AcceptanceTestDriver) WriteToSource(t *testing.T, records []sdk.Record) []sdk.Record {
	if d.Connector().NewDestination == nil {
		t.Fatal("connector is missing the field NewDestination, either implement the destination or overwrite the driver method Write")
	}

	is := is.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// writing something to the destination should result in the same record
	// being produced by the source
	dest := d.Connector().NewDestination()
	// write to source and not the destination
	destConfig := d.SourceConfig(t)
	err := dest.Configure(ctx, destConfig)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)
	records = d.generateRecords(len(records))
	// try to write using WriteAsync and fallback to Write if it's not supported
	err = d.write(ctx, dest, records)
	is.NoErr(err)

	cancel() // cancel context to simulate stop
	err = dest.Teardown(context.Background())
	is.NoErr(err)

	return records
}

// write writes records to destination using Destination.Write.
func (d AcceptanceTestDriver) write(ctx context.Context, dest sdk.Destination, records []sdk.Record) error {
	for _, r := range records {
		err := dest.Write(ctx, r)
		if err != nil {
			return err
		}
	}

	// flush to make sure the records get written to the destination, but allow
	// it to be unimplemented
	err := dest.Flush(ctx)
	if err != nil && !errors.Is(err, sdk.ErrUnimplemented) {
		return err
	}

	// records were successfully written
	return nil
}

func (d AcceptanceTestDriver) generateRecords(count int) []sdk.Record {
	records := make([]sdk.Record, count)
	for i := range records {
		records[i] = d.generateRecord()
	}
	return records
}

func (d AcceptanceTestDriver) generateRecord() sdk.Record {
	return sdk.Record{
		Metadata: map[string]string{
			"key": sourceKey,
		},
		CreatedAt: time.Time{},
		Key:       sdk.RawData(sourceKey),
		Payload:   sdk.RawData(fmt.Sprintf(`{"%s":"%s"}`, d.randString(32), d.randString(32))),
	}
}

// randString generates a random string of length n.
// (source: https://stackoverflow.com/a/31832326)
func (d AcceptanceTestDriver) randString(n int) string {
	const letterBytes = `0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz`
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)
	sb := strings.Builder{}
	sb.Grow(n)
	// src.Int63() generates 63 random bits, enough for letterIdxMax characters
	for i, cache, remain := n-1, d.rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = d.rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return sb.String()
}
