package cmd

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/magefile/mage/sh"
	"github.com/onedaycat/zamus/zamus/config"
	"github.com/plimble/mage/mg"
	"github.com/spf13/cobra"
)

var ciSave bool
var ciBitbucket bool

func init() {
	CICmd.Flags().BoolVarP(&ciSave, "save", "s", false, "Save current commit after run (DEFAULT: false)")
	CICmd.Flags().BoolVarP(&ciBitbucket, "bitbucket", "b", false, "Use BITBUCKET_COMMIT for the lastest commit (DEFAULT: false)")
}

var CICmd = &cobra.Command{
	Use:   "ci",
	Short: "Watch dor changed for ci",
	Long:  "Watch dor changed for ci\nif last commit from filepath is not found, ci will run all steps\nargs[0]=<step_name>, [args[1]=<current_commit>",
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
				notExist = true
			}
		}

		if !notExist {
			fmt.Println("Last commit loaded:", string(lastCommit))
		}

		var curCommit string
		if ciBitbucket {
			bitCom := os.Getenv("BITBUCKET_COMMIT")
			if bitCom != "" {
				fmt.Println("Bitbucket commit found:", bitCom)
				curCommit = bitCom
			} else {
				fmt.Println("Current commit from bitbucket not found use last commit from args instead")
			}
		}

		if len(args) < 1 {
			fmt.Println("Error:", "step name args is required")
			os.Exit(1)
		}

		if curCommit == "" {
			if len(args) < 2 {
				fmt.Println("Error:", "require 2 arguments, <current_commit> is missing")
				os.Exit(1)
			} else {
				curCommit = args[0]
			}
		}

		if curCommit == string(lastCommit) {
			fmt.Println("Last and current commit are equal")
			fmt.Println("Nothing changed!!!")
			os.Exit(0)
		}

		step, ok := config.C.CI.Steps[args[0]]
		if !ok {
			fmt.Println("Step", args[0], "is not found")
			os.Exit(0)
		} else {
			fmt.Println("Start", args[0], "step!")
		}

		if notExist {
			fmt.Println("Run all scripts!")
		}

		for _, folders := range config.C.CI.Folders {
			fmt.Println("\nRun", args[0], "step on", folders)

			if !notExist {
				gitArgs := make([]string, 0, 5+len(folders))
				gitArgs = append(gitArgs, "diff", "--name-only", string(lastCommit), curCommit, "--")
				gitArgs = append(gitArgs, folders...)
				result, _ := sh.Output("git", gitArgs...)
				if len(result) > 0 {
					fmt.Println("Found changed!!!")
					runSteps = true
				} else {
					runSteps = false
					fmt.Println("Nothing changed!!!")
				}
			}

			if runSteps {
				for _, script := range step {
					mg.Exec(script)
				}
			}
		}

		if ciSave {
			ioutil.WriteFile(config.C.CI.Fileapath, []byte(curCommit), 0644)
			fmt.Println("\nSave commit:", curCommit)
		}

		fmt.Println("\nDone!!!")
	},
}
