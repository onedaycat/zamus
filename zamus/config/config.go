package config

var C Config

type Config struct {
	Migration *Migration `yaml:"migration"`
	Deploy    *Deploy    `yaml:"deploy"`
}

type Migration struct {
	Datasource string `yaml:"datasource"`
	Dir        string `yaml:"dir"`
}

type Deploy struct {
	Fileapath string         `yaml:"filepath"`
	Watch     []*DeployWatch `yaml:"watch"`
}

type DeployWatch struct {
	Folders []string            `yaml:"folders"`
	Steps   map[string][]string `yaml:"steps"`
}

func init() {
	C = Config{}
}
