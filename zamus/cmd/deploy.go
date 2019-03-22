package cmd

import (
	"fmt"
	"io/ioutil"
	"os"

	jsoniter "github.com/json-iterator/go"
	"github.com/magefile/mage/sh"
	"github.com/onedaycat/zamus/zamus/config"
	"github.com/plimble/mage/mg"
	"github.com/spf13/cobra"
)

var deploySave bool
var deployAll bool

func init() {
	DeployCmd.Flags().BoolVarP(&deploySave, "save", "s", false, "Save current commit after run (DEFAULT: false)")
	DeployCmd.Flags().BoolVarP(&deployAll, "all", "a", false, "Run all script, ignore file changed (DEFAULT: false)")
}

var DeployCmd = &cobra.Command{
	Use:   "deploy <branch> <step> [<current_commit>]",
	Short: "Deploy only folder chnaged",
	Long:  `Deploy only folder chnaged\nif last commit from filepath is not found, deploy will run all steps`,
	Args:  cobra.MinimumNArgs(3),
	Run: func(cmd *cobra.Command, args []string) {
		notExist := false
		runSteps := true
		commits := make(map[string]string)
		var curCommit string
		var lastCommit string
		var commitjson []byte
		var err error

		curCommit = args[2]

		if deployAll {
			notExist = true
			goto StartScript
		}

		commitjson, err = ioutil.ReadFile(config.C.Deploy.Fileapath)
		fmt.Println("Load last commit from:", config.C.Deploy.Fileapath)

		if err != nil {
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
			jsoniter.ConfigFastest.Unmarshal(commitjson, &commits)
			lastCommit = commits[args[0]]
			if lastCommit == "" {
				notExist = true
				fmt.Println("No last commit on for", args[0])
			} else {
				fmt.Println("Last commit loaded:", lastCommit)
			}
		}

		if curCommit == lastCommit {
			fmt.Println("Last and current commit are equal")
			fmt.Println("Skip deploy!!!")
			os.Exit(0)
		}

	StartScript:
		if notExist {
			fmt.Println("Run all scripts!")
		}

		for _, watch := range config.C.Deploy.Watch {
			fmt.Println("\nRun", args[1], "step on", watch.Folders)

			if !notExist {
				gitArgs := make([]string, 0, 5+len(watch.Folders))
				gitArgs = append(gitArgs, "diff", "--name-only", lastCommit, curCommit, "--")
				gitArgs = append(gitArgs, watch.Folders...)
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
				step, ok := watch.Steps[args[1]]
				if !ok {
					fmt.Println("Step", args[1], "is not found")
					continue
				} else {
					fmt.Println("Start", args[1], "step!")
					for _, script := range step {
						mg.Exec(script)
					}
				}
			}
		}

		if deploySave {
			commits[args[0]] = curCommit
			commitjson, _ = jsoniter.ConfigFastest.MarshalIndent(commits, "", "  ")

			ioutil.WriteFile(config.C.Deploy.Fileapath, commitjson, 0644)
			fmt.Println("\nSave commit:", curCommit)
		}

		fmt.Println("\nDone!!!")
	},
}
