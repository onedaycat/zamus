package config

var C Config

type Config struct {
	Migration *Migration `yaml:"migration"`
}

type Migration struct {
	Datasource string `yaml:"datasource"`
	Dir        string `yaml:"dir"`
}

func init() {
	C = Config{}
}
