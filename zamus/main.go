package main

import (
	"fmt"
	"os"

	"github.com/onedaycat/zamus/zamus/cmd"
	"github.com/onedaycat/zamus/zamus/config"
	"github.com/plimble/goconf"
	"github.com/spf13/cobra"
)

var cfgFile string

func main() {
	cobra.OnInitialize(initConfig)
	var rootCmd = &cobra.Command{
		Use:     "zamus",
		Version: "v0.53.0",
	}
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./zamus.yml)")
	rootCmd.AddCommand(cmd.DeployCmd)
	_ = rootCmd.Execute()
}

func initConfig() {
	if err := goconf.Parse(&config.C, goconf.WithEnv("zamus")); err != nil {
		fmt.Println("Can't read config:", err)
		os.Exit(1)
	}

	if cfgFile != "" {
		if err := goconf.Parse(&config.C, goconf.WithYaml(cfgFile)); err != nil {
			fmt.Println("Can't read config:", err)
			os.Exit(1)
		}
	}

	if _, err := os.Stat("zamus.yml"); err == nil {
		if err := goconf.Parse(&config.C, goconf.WithYaml("zamus.yml")); err != nil {
			fmt.Println("Can't read config:", err)
			os.Exit(1)
		}
	}
}
