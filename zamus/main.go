package main

import (
	"fmt"
	"os"

	"gopkg.in/plimble/goconf.v1"

	"github.com/onedaycat/zamus/zamus/cmd"
	"github.com/onedaycat/zamus/zamus/config"
	"github.com/spf13/cobra"
)

var cfgFile string

func main() {
	cobra.OnInitialize(initConfig)
	var rootCmd = &cobra.Command{
		Use:     "zamus",
		Version: "v1.0.5",
	}
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./zamus.yml)")
	rootCmd.AddCommand(cmd.MigrateUpCmd)
	rootCmd.AddCommand(cmd.MigrateDownCmd)
	rootCmd.Execute()
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

	// viper.SetEnvPrefix("zamus")
	// viper.AutomaticEnv()
	// viper.SetConfigType("yaml")
	// if cfgFile != "" {
	// 	viper.SetConfigFile(cfgFile)
	// } else {
	// 	viper.SetConfigName("zamus")
	// 	viper.AddConfigPath(".")
	// }

	// if err := viper.ReadInConfig(); err != nil {
	// 	fmt.Println("Can't read config:", err)
	// 	os.Exit(1)
	// }

	// if err := viper.Unmarshal(&config.C); err != nil {
	// 	fmt.Println("Can't read config:", err)
	// 	os.Exit(1)
	// }
}
