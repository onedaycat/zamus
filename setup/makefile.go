// +build mage

package main

import (
	"fmt"

	"github.com/plimble/mage/mg"
)

func Build() {
	mg.BuildLinux("./dynamokinesis", "./dynamokinesis/bin/app")
	mg.BuildLinux("./firehosetransform", "./firehosetransform/bin/app")
	fmt.Println("Build Done")
}

func Deploy() {
	Build()
	mg.Exec("serverless deploy -v")
}

func Remove() {
	mg.Exec("serverless remove -v")
}
