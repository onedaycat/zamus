// +build mage

package main

import (
    "fmt"

    "github.com/plimble/mage/mg"
)

func Release() {
    ver := "v0.80.0"
    mg.BuildLinux("./setup/eventsource", "./setup/eventsource/bin/app")
    mg.BuildLinux("./setup/dlq", "./setup/dlq/bin/app")
    mg.ExecX("zip ./eventsource.zip app").Dir("./setup/eventsource/bin").Run()
    mg.ExecX("zip ./dlq.zip app").Dir("./setup/dlq/bin").Run()
    mg.ExecX(fmt.Sprintf("hub release create -a ./setup/eventsource/bin/eventsource.zip -a ./setup/dlq/bin/dlq.zip -m %s %s", ver, ver)).Run()
}

type Deploy mg.Namespace

func (Deploy) EventSource() {
    mg.BuildLinux("./setup/eventsource", "./setup/eventsource/bin/app")
    //mg.ExecX("serverless deploy -v").Dir("setup/eventsource").Run()
}

func (Deploy) DLQ() {
    mg.BuildLinux("./setup/dlq", "./setup/dlq/bin/app")
    mg.ExecX("serverless deploy -v").Dir("setup/dlq").Run()
}

func (Deploy) Saga() {
    mg.BuildLinux("./setup/saga", "./setup/saga/bin/app")
    mg.ExecX("serverless deploy -v").Dir("setup/saga").Run()
}

type Remove mg.Namespace

func (Remove) EventSource() {
    mg.ExecX("serverless remove -v").Dir("setup/eventsource").Run()
}

func (Remove) DLQ() {
    mg.ExecX("serverless remove -v").Dir("setup/dlq").Run()
}

func (Remove) Saga() {
    mg.ExecX("serverless remove -v").Dir("setup/saga").Run()
}

func Proto() {
    mg.ExecX("protoc --gogofast_out=. event/event.proto").Run()
}
