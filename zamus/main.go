package main

import (
	"fmt"
	"os"

	"github.com/onedaycat/zamus/zamus/cmd"
	"github.com/onedaycat/zamus/zamus/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	viper.SetConfigType("yaml")
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("zamus")
		viper.AddConfigPath(".")
	}

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("Can't read config:", err)
		os.Exit(1)
	}

	if err := viper.Unmarshal(&config.C); err != nil {
		fmt.Println("Can't read config:", err)
		os.Exit(1)
	}
}
