// +build mage

package main

import (
    "fmt"

    "github.com/plimble/mage/mg"
)

func Build() {
    mg.BuildLinux("./setup/eventsource", "./setup/eventsource/bin/app")
    fmt.Println("Build Done")
}

type Deploy mg.Namespace

func (Deploy) EventSource() {
    Build()
    mg.ExecX("serverless deploy -v").Dir("setup/eventsource").Run()
}

type Remove mg.Namespace

func (Remove) EventSource() {
    mg.ExecX("serverless remove -v").Dir("setup/eventsource").Run()
}
