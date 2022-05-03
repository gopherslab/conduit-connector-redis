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

package config

import (
	"fmt"
	"time"
)

const (
	KeyHost          = "host"
	KeyPort          = "port"
	KeyRedisKey      = "key"
	KeyDatabase      = "database"
	KeyPassword      = "password"
	KeyMode          = "mode"
	KeyPollingPeriod = "polling_period"
)

type Config struct {
	Host          string
	Port          string
	Database      string
	Key           string
	Password      string
	Mode          Mode
	PollingPeriod time.Duration
}
type Mode string

const (
	ModePubSub Mode = "pubsub"
	ModeStream Mode = "stream"
)

var modeAll = []string{string(ModePubSub), string(ModeStream)}

func Parse(cfg map[string]string) (Config, error) {
	host, ok := cfg[KeyHost]
	if !ok {
		host = "localhost"
	}
	port, ok := cfg[KeyPort]
	if !ok {
		port = "6379"
	}
	key, ok := cfg[KeyRedisKey]
	if !ok {
		return Config{}, requiredConfigErr(KeyRedisKey)
	}
	pollingPeriod, ok := cfg[KeyPollingPeriod]
	if !ok {
		pollingPeriod = "1s"
	}
	pollingDuration, err := time.ParseDuration(pollingPeriod)
	if err != nil {
		return Config{}, fmt.Errorf("invalid polling duration passed(%v)", pollingPeriod)
	}
	config := Config{
		Host:          host,
		Key:           key,
		Port:          port,
		Database:      cfg[KeyDatabase],
		Password:      cfg[KeyPassword],
		Mode:          ModePubSub,
		PollingPeriod: pollingDuration,
	}
	if modeRaw := cfg[KeyMode]; modeRaw != "" {
		if !isModeSupported(modeRaw) {
			return Config{}, fmt.Errorf("%q contains unsupported value %q, expected one of %v", KeyMode, modeRaw, modeAll)
		}
		config.Mode = Mode(modeRaw)
	}
	return config, nil
}

func isModeSupported(modeRaw string) bool {
	for _, m := range modeAll {
		if m == modeRaw {
			return true
		}
	}
	return false
}
func requiredConfigErr(name string) error {
	return fmt.Errorf("%q config value must be set", name)
}
