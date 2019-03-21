package config

var C Config

type Config struct {
	Migration *Migration `yaml:"migration"`
	CI        *CI        `yaml:"ci"`
}

type Migration struct {
	Datasource string `yaml:"datasource"`
	Dir        string `yaml:"dir"`
}

type CI struct {
	Fileapath string       `yaml:"filepath"`
	Trigger   []*CITrigger `yaml:"trigger"`
}

type CITrigger struct {
	Name    string   `yaml:"name"`
	Folders []string `yaml:"folders"`
	Script  []string `yaml:"script"`
}

func init() {
	C = Config{}
}
