// +build mage

package main

import (
    "github.com/plimble/mage/mg"
)

func Generate() {
    mg.ExecX("go generate ./...").Run()
}

func Test() {
    mg.ExecX("go test -tags integration ./...").Run()
}

func Dep() {
    mg.ExecX("go get -u ./...").Run()
    mg.ExecX("go mod tidy").Run()
}
