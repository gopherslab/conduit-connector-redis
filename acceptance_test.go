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

package redis

import (
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
	"go.uber.org/goleak"
)

const (
	key = "dummy_key"
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
		config.KeyRedisKey:      key,
		config.KeyMode:          string(config.ModeStream),
		config.KeyPollingPeriod: time.Millisecond.String(),
	}
	destConf := map[string]string{
		config.KeyHost:     mr.Host(),
		config.KeyPort:     mr.Port(),
		config.KeyRedisKey: key,
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

// GenerateRecord overrides the pre-defined generate record function to generate the records in required format
// Sample Record:
// {
//	"metadata": {
//		"key": "dummy_key"
//	},
//	"created_at": "0001-01-01 00:00:00 +0000 UTC",
//	"key": "dummy_key",
//	"payload": "{\"key\":\"value\"}"
//}
func (d AcceptanceTestDriver) GenerateRecord(t *testing.T) sdk.Record {
	return sdk.Record{
		Metadata: map[string]string{
			"key": key,
		},
		CreatedAt: time.Time{},
		Key:       sdk.RawData(key),
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
