package random

import (
	"strings"

	jsoniter "github.com/json-iterator/go"

	random "github.com/Pallinder/go-randomdata"
	"github.com/onedaycat/zamus/command"
	"github.com/onedaycat/zamus/invoke"
)

type commandBuilder struct {
	req *command.CommandReq
}

func CommandReq() *commandBuilder {
	username := random.Noun()
	email := random.Email()
	ip := random.IpV4Address()
	return &commandBuilder{
		req: &command.CommandReq{
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
	b.req.Function = fn

	return b
}

func (b *commandBuilder) Arg(v interface{}) *commandBuilder {
	b.req.WithArgs(v)

	return b
}

func (b *commandBuilder) Source(v interface{}) *commandBuilder {
	b.req.WithSource(v)

	return b
}

func (b *commandBuilder) ValidPermission(key string, permissions ...string) *commandBuilder {
	b.req.PermissionKey = key
	b.req.Identity.Claims.Permissions[key] = strings.Join(permissions, ",")

	return b
}

func (b *commandBuilder) InvalidPermission() *commandBuilder {
	pem := random.SillyName()
	b.req.PermissionKey = pem
	b.req.Identity.Claims.Permissions[pem] = strings.Join([]string{random.SillyName(), random.SillyName()}, ",")

	return b
}

func (b *commandBuilder) NoIdentity() *commandBuilder {
	b.req.PermissionKey = ""
	b.req.Identity = nil

	return b
}

func (b *commandBuilder) Build() *command.CommandReq {
	return b.req
}

func (b *commandBuilder) BuildJSON() []byte {
	data, err := jsoniter.ConfigFastest.Marshal(b.req)
	if err != nil {
		panic(err)
	}

	return data
}
