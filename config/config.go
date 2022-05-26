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
	KeyHost          = "redis.host"
	KeyPort          = "redis.port"
	KeyRedisKey      = "redis.key"
	KeyDatabase      = "redis.database"
	KeyUsername      = "redis.username"
	KeyPassword      = "redis.password"
	KeyMode          = "mode"
	KeyPollingPeriod = "pollingPeriod"

	defaultHost          = "localhost"
	defaultPort          = "6379"
	defaultPollingPeriod = "1s"
	defaultDatabase      = "0"
)

type Config struct {
	// Host is the redis host to connect to. default is localhost
	Host string
	// Port is the redis port to connect to. default is 6379
	Port string
	// Username to be used for connecting to redis. default is ""
	Username string
	// Password to be used for connecting to redis. default is ""
	Password string // Database is an optional parameter used for connecting to a specified database number
	// default is 0
	Database int
	// RedisKey is the redis key that we want to track
	// This config expects a valid key name for ModeStream and the key should be of type none or stream
	// Check the key type in redis using `TYPE <key>`.
	// For ModePubSub, this config expects a valid channel name to subscribe to.
	// There is no key type for channels in redis and a channel can have same name as an existing key of DS type in redis.
	RedisKey string
	// Mode can be thought of as the redis key type, it is used to start the corresponding iterator.
	// Since there is no type for pubsub channels, it is not possible to decide which iterator to run at runtime
	// by using the `TYPE key` command, furthermore, the channel name can have a key of some other type existing in the redis.
	// So, it is required to add this detail manually during configuration
	Mode Mode
	// PollingPeriod is only used for source connector in stream mode
	// This period is used by StreamIterator to poll for new data at regular intervals.
	PollingPeriod time.Duration
}

// Mode is the type used to supply the type of redis.key supplied in config, it is used to start corresponding iterator
type Mode string

const (
	ModePubSub Mode = "pubsub"
	ModeStream Mode = "stream"
)

var modeAll = []string{string(ModePubSub), string(ModeStream)}

// Parse parses and validates the supplied config
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
		RedisKey:      key,
		Port:          port,
		Database:      dbInt,
		Password:      cfg[KeyPassword],
		Username:      cfg[KeyUsername],
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

// isModeSupported is used to validate the supplied mode string
func isModeSupported(modeRaw string) bool {
	for _, m := range modeAll {
		if m == modeRaw {
			return true
		}
	}
	return false
}

// requiredConfigErr is a helper function to generate required config error
func requiredConfigErr(name string) error {
	return fmt.Errorf("%q config value must be set", name)
}
