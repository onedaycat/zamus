package random

import (
	"strings"

	jsoniter "github.com/json-iterator/go"

	random "github.com/Pallinder/go-randomdata"
	"github.com/onedaycat/zamus/command"
	"github.com/onedaycat/zamus/invoke"
)

type commandBuilder struct {
	cmd *command.Command
}

func Command() *commandBuilder {
	username := random.Noun()
	email := random.Email()
	ip := random.IpV4Address()
	return &commandBuilder{
		cmd: &command.Command{
			Function: random.SillyName(),
			Identity: &invoke.Identity{
				Sub:      username,
				SourceIP: []string{ip},
				Groups:   []string{"user"},
				Username: username,
				Claims: invoke.Claims{
					Email:       email,
					Permissions: make(invoke.Permissions),
				},
			},
		},
	}
}

func (b *commandBuilder) Function(fn string) *commandBuilder {
	b.cmd.Function = fn

	return b
}

func (b *commandBuilder) Arg(v interface{}) *commandBuilder {
	data, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(v)
	if err != nil {
		panic(err)
	}

	b.cmd.Args = data

	return b
}

func (b *commandBuilder) Source(v interface{}) *commandBuilder {
	data, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(v)
	if err != nil {
		panic(err)
	}

	b.cmd.Source = data

	return b
}

func (b *commandBuilder) ValidPermission(key string, permissions ...string) *commandBuilder {
	b.cmd.PermissionKey = key
	b.cmd.Identity.Claims.Permissions[key] = strings.Join(permissions, ",")

	return b
}

func (b *commandBuilder) InvalidPermission() *commandBuilder {
	pem := random.SillyName()
	b.cmd.PermissionKey = pem
	b.cmd.Identity.Claims.Permissions[pem] = strings.Join([]string{random.SillyName(), random.SillyName()}, ",")

	return b
}

func (b *commandBuilder) NoIdentity() *commandBuilder {
	b.cmd.PermissionKey = ""
	b.cmd.Identity = nil

	return b
}

func (b *commandBuilder) Build() *command.Command {
	return b.cmd
}

func (b *commandBuilder) BuildJSON() []byte {
	data, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(b.cmd)
	if err != nil {
		panic(err)
	}

	return data
}
