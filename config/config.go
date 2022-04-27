package config

import (
	"fmt"
)

const (
	ConfigKeyHost     = "host"
	ConfigKeyPort     = "port"
	ConfigKeyKey      = "redis_key"
	ConfigKeyDatabase = "database"
	ConfigKeyPassword = "password"
	ConfigKeyChannel  = "channel"
	ConfigKeyMode     = "mode"
)

type Config struct {
	Host     string
	Port     string
	Database string
	Key      string
	Password string
	Channel  string
	Mode     Mode
}
type Mode string

const (
	ModePubSub string = "pubsub"
	ModeStream string = "stream"
)

var modeAll = []string{ModePubSub, ModeStream}

func Parse(cfg map[string]string) (Config, error) {
	host, ok := cfg[ConfigKeyHost]
	if !ok {
		return Config{}, requiredConfigErr(ConfigKeyHost)
	}
	port, ok := cfg[ConfigKeyPort]
	if !ok {
		return Config{}, requiredConfigErr(ConfigKeyPort)
	}
	config := Config{
		Host:     host,
		Key:      cfg[ConfigKeyKey],
		Port:     port,
		Database: cfg[ConfigKeyDatabase],
		Password: cfg[ConfigKeyPassword],
		Channel:  cfg[ConfigKeyChannel],
		Mode:     Mode(ModePubSub),
	}
	if modeRaw := cfg[ConfigKeyMode]; modeRaw != "" {
		if !isModeSupported(modeRaw) {
			return Config{}, fmt.Errorf("%q contains unsupported value %q, expected one of %v", ConfigKeyMode, modeRaw, modeAll)
		}
		config.Mode = Mode(modeRaw)
	}
	return config, nil
}

func isModeSupported(modeRaw string) bool {
	for _, m := range modeAll {
		if string(m) == modeRaw {
			return true
		}
	}
	return false
}
func requiredConfigErr(name string) error {
	return fmt.Errorf("%q config value must be set", name)
}
