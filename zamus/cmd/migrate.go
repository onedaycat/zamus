package cmd

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/onedaycat/zamus/zamus/config"
	"github.com/onedaycat/zamus/zamus/migration"
	"github.com/spf13/cobra"
)

var MigrateUpCmd = &cobra.Command{
	Use:   "migrate-up",
	Short: "Migrate sql tear-up",
	Long:  `Migrate sql with tear-up`,
	Run: func(cmd *cobra.Command, args []string) {
		var ms []config.Migration
		if len(args) > 0 {
			ms = make([]config.Migration, 0, len(args))
			for i := 0; i < len(args); i++ {
				for _, xm := range config.C.Migrations {
					if xm.Name == args[i] {
						ms = append(ms, xm)
					}
				}
			}
		} else {
			ms = config.C.Migrations
		}

		if len(ms) == 0 {
			fmt.Println("No migration found!")
			os.Exit(0)
		}

		for _, m := range ms {
			fmt.Println("Start migrate", m.Name)
			datasource := m.Datasource
			dir := m.Dir
			db, err := sql.Open("mysql", datasource)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			n, err := migration.Up(db, dir)
			if err != nil {
				fmt.Println(err)
				db.Close()
				os.Exit(1)
			}
			fmt.Printf("Applied %d migrations up!\n", n)
			db.Close()
		}
	},
}

var MigrateDownCmd = &cobra.Command{
	Use:   "migrate-down",
	Short: "Migrate sql tear-down",
	Long:  `Migrate sql with tear-down`,
	Run: func(cmd *cobra.Command, args []string) {
		var ms []config.Migration
		if len(args) > 0 {
			ms = make([]config.Migration, 0, len(args))
			for i := 0; i < len(args); i++ {
				for _, xm := range config.C.Migrations {
					if xm.Name == args[i] {
						ms = append(ms, xm)
					}
				}
			}
		} else {
			ms = config.C.Migrations
		}

		if len(ms) == 0 {
			fmt.Println("No migration found!")
			os.Exit(0)
		}

		for _, m := range ms {
			fmt.Println("Start migrate", m.Name)
			datasource := m.Datasource
			dir := m.Dir
			db, err := sql.Open("mysql", datasource)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			n, err := migration.Down(db, dir)
			if err != nil {
				fmt.Println(err)
				db.Close()
				os.Exit(1)
			}
			fmt.Printf("Applied %d migrations down!\n", n)
			db.Close()
		}
	},
}
