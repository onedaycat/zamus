package config

var C Config

type Config struct {
	Deploy *Deploy `yaml:"deploy"`
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
