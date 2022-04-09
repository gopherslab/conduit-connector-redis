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
)

type Config struct {
	Host     string
	Port     string
	Database string
	Key      string
	Password string
}

func Parse(cfg map[string]string) (Config, error) {
	host, ok := cfg[ConfigKeyHost]
	if !ok {
		return Config{}, requiredConfigErr(ConfigKeyHost)
	}
	key, ok := cfg[ConfigKeyKey]
	if !ok {
		return Config{}, requiredConfigErr(ConfigKeyKey)
	}
	port, ok := cfg[ConfigKeyPort]
	if !ok {
		return Config{}, requiredConfigErr(ConfigKeyPort)
	}
	database, ok := cfg[ConfigKeyDatabase]
	if !ok {
		return Config{}, requiredConfigErr(ConfigKeyDatabase)
	}
	config := Config{
		Host:     host,
		Key:      key,
		Port:     port,
		Database: database,
		Password: cfg[ConfigKeyPassword],
	}

	return config, nil
}

func requiredConfigErr(name string) error {
	return fmt.Errorf("%q config value must be set", name)
}
