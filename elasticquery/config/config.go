package config

// GeneralConfig holds the entire configuration
type GeneralConfig struct {
	ESConfig ElasticInstanceConfig `toml:"config"`
}

// ElasticInstanceConfig holds the configuration needed for connecting to an Elasticsearch instance
type ElasticInstanceConfig struct {
	URL      string `toml:"url"`
	Username string `toml:"username"`
	Password string `toml:"password"`
}
