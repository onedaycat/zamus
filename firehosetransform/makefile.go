// +build mage

package main

import (
	"fmt"

	"github.com/plimble/mage/mg"
)

type Build mg.Namespace

func (Build) Linux() {
	mg.BuildLinux(".", "./bin/app")
	fmt.Println("Build Done")
}

func (Build) Mac() {
	mg.BuildMac(".", "./bin/app")
	fmt.Println("Build Done")
}

func Deploy() {
	Build{}.Linux()
	mg.Exec("serverless deploy -v")
}

func Remove() {
	mg.Exec("serverless remove -v")
}
