package config

var C Config

type Config struct {
	Migrations []Migration `yaml:"migrations"`
}

type Migration struct {
	Name       string `yaml:"name"`
	Datasource string `yaml:"datasource"`
	Dir        string `yaml:"dir"`
}

func init() {
	C = Config{}
}
