package config

var C Config

type Config struct {
	Migration     *Migration `yaml:"migration"`
	DqlDataSource string     `taml:"dqlDatasource" envconfig:"DQL_DATASOURCE"`
}

type Migration struct {
	Datasource string `yaml:"datasource"`
	Dir        string `yaml:"dir"`
}

func init() {
	C = Config{}
}
