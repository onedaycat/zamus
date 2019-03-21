package cmd

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/magefile/mage/sh"
	"github.com/onedaycat/zamus/zamus/config"
	"github.com/plimble/mage/mg"
	"github.com/spf13/cobra"
)

var ciSave bool

func init() {
	CICmd.Flags().BoolVarP(&ciSave, "save", "s", false, "Save current commit after run (DEFAULT: false)")
}

var CICmd = &cobra.Command{
	Use:       "ci",
	Short:     "Watch dor changed for ci",
	Long:      "Watch dor changed for ci\nif last commit from filepath is not found, ci will run all steps",
	ValidArgs: []string{"last commit", "current commit"},
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.New("arguments is required <current_commit>")
		}

		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		lastCommit, err := ioutil.ReadFile(config.C.CI.Fileapath)
		fmt.Println("Load last commit from:", config.C.CI.Fileapath)

		notExist := false
		runSteps := true
		if err != nil && !notExist {
			if notExist = os.IsNotExist(err); !notExist {
				fmt.Println(err)
				os.Exit(1)
				return
			} else {
				fmt.Println("Last commit not found!")
				fmt.Println("Run all scripts!")
			}
		}

		if !notExist {
			fmt.Println("Last commit loaded:", string(lastCommit))
		}

		trigger := config.C.CI.Trigger
		for i := range trigger {
			fmt.Println("\nCI Run:", trigger[i].Name)

			if !notExist {
				gitArgs := make([]string, 0, 5+len(trigger[i].Folders))
				gitArgs = append(gitArgs, "diff", "--name-only", string(lastCommit), args[0], "--")
				gitArgs = append(gitArgs, trigger[i].Folders...)
				result, _ := sh.Output("git", gitArgs...)
				if len(result) > 0 {
					fmt.Println("Found changed!!!", trigger[i].Folders)
					runSteps = true
				} else {
					runSteps = false
					fmt.Println("Nothing changed on", trigger[i].Folders)
				}
			}

			if runSteps {
				for _, sc := range trigger[i].Script {
					mg.Exec(sc)
				}
			}
		}

		if ciSave {
			ioutil.WriteFile(config.C.CI.Fileapath, []byte(args[0]), 0644)
			fmt.Println("\nSave commit:", args[0])
		}

		// fmt.Println("@@", err)
		// fmt.Println(len(result))
	},
}

// --full-index
// --binary
// -a
// --exit-code
