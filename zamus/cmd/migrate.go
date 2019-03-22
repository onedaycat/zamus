package cmd

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/onedaycat/zamus/zamus/migration"
	"github.com/spf13/cobra"
)

var upMax int
var downMax int

func init() {
	MigrateUpCmd.Flags().IntVarP(&upMax, "max", "m", 0, "Set max step to migrate (DEFAULT: 0 (unlimit))")
	MigrateDownCmd.Flags().IntVarP(&downMax, "max", "m", 0, "Set max step to migrate (DEFAULT: 0 (unlimit))")
}

var MigrateUpCmd = &cobra.Command{
	Use:   "migrate-up <source> <migration_folder>",
	Short: "Migrate sql tear-up",
	Long:  `Migrate sql with tear-up`,
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		datasource := args[0]
		dir := args[1]
		fmt.Printf("Start migrating max(%d)\n", upMax)
		db, err := sql.Open("mysql", datasource)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		n, err := migration.Up(db, dir, upMax)
		if err != nil {
			fmt.Println(err)
			db.Close()
			os.Exit(1)
		}
		fmt.Printf("Applied %d migrations up!\n", n)
		db.Close()
	},
}

var MigrateDownCmd = &cobra.Command{
	Use:   "migrate-down <source> <migration_folder>",
	Short: "Migrate sql tear-down",
	Long:  `Migrate sql with tear-down`,
	Run: func(cmd *cobra.Command, args []string) {
		datasource := args[0]
		dir := args[1]
		fmt.Printf("Start migrating max(%d)\n", downMax)
		db, err := sql.Open("mysql", datasource)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		n, err := migration.Down(db, dir, downMax)
		if err != nil {
			fmt.Println(err)
			db.Close()
			os.Exit(1)
		}
		fmt.Printf("Applied %d migrations up!\n", n)
		db.Close()
	},
}
