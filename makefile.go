// +build mage

package main

import (
	"github.com/plimble/mage/mg"
)

type Deploy mg.Namespace

func (Deploy) EventSource() {
	mg.BuildLinux("./setup/eventsource", "./setup/eventsource/bin/app")
	mg.ExecX("serverless deploy -v").Dir("setup/eventsource").Run()
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
