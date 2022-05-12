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
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/conduitio/conduit-connector-redis/destination"
	"github.com/conduitio/conduit-connector-redis/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.uber.org/goleak"
)

func TestAcceptance(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	sourceConf := map[string]string{
		"redis.host": mr.Host(),
		"redis.port": mr.Port(),
		"redis.key":  "dummy_key_source",
		"mode":       "stream",
	}
	destConf := map[string]string{
		"redis.host": mr.Host(),
		"redis.port": mr.Port(),
		"redis.key":  "dummy_key_dest",
		"mode":       "stream",
	}
	// Add a key value pair for source to read
	if _, err := mr.XAdd("dummy_key_source", "*", []string{"key", "value"}); err != nil {
		t.Fatal(err)
	}

	sdk.AcceptanceTest(t, sdk.ConfigurableAcceptanceTestDriver{
		Config: sdk.ConfigurableAcceptanceTestDriverConfig{
			Connector: sdk.Connector{ // Note that this variable should rather be created globally in `connector.go`
				NewSpecification: Specification,
				NewSource:        source.NewSource,
				NewDestination:   destination.NewDestination,
			},
			SourceConfig:      sourceConf,
			DestinationConfig: destConf,
			GoleakOptions:     []goleak.Option{goleak.IgnoreCurrent()},
			Skip: []string{
				// these tests are skipped, because they need valid json of type map[string]string to work
				// whereas the code generates random string payload
				"TestDestination_Write_Success",
				"TestSource_Open_ResumeAtPosition",
				"TestSource_Read_Success",
			},
		},
	})
}
