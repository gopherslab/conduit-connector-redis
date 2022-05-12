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
	"strconv"
	"time"
)

const (
	KeyHost              = "redis.host"
	KeyPort              = "redis.port"
	KeyRedisKey          = "redis.key"
	KeyDatabase          = "redis.database"
	KeyPassword          = "redis.password"
	KeyMode              = "mode"
	KeyPollingPeriod     = "pollingPeriod"
	defaultHost          = "localhost"
	defaultPort          = "6379"
	defaultPollingPeriod = "1s"
	defaultDatabase      = "0"
)

type Config struct {
	Host          string
	Port          string
	Database      int
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
		host = defaultHost
	}

	port, ok := cfg[KeyPort]
	if !ok {
		port = defaultPort
	}

	key, ok := cfg[KeyRedisKey]
	if !ok {
		return Config{}, requiredConfigErr(KeyRedisKey)
	}

	pollingPeriod, ok := cfg[KeyPollingPeriod]
	if !ok {
		pollingPeriod = defaultPollingPeriod
	}

	pollingDuration, err := time.ParseDuration(pollingPeriod)
	if err != nil {
		return Config{}, fmt.Errorf("invalid polling duration passed(%v)", pollingPeriod)
	}

	db, ok := cfg[KeyDatabase]
	if !ok || db == "" {
		db = defaultDatabase
	}

	dbInt, err := strconv.Atoi(db)
	if err != nil {
		return Config{}, fmt.Errorf("invalid database passed, should be a valid int")
	}

	config := Config{
		Host:          host,
		Key:           key,
		Port:          port,
		Database:      dbInt,
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
