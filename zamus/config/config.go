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
	Fileapath string              `yaml:"filepath"`
	Folders   [][]string          `yaml:"folders"`
	Steps     map[string][]string `yaml:"steps"`
}

func init() {
	C = Config{}
}
