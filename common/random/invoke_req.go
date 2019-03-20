package random

import (
	"github.com/onedaycat/zamus/invoke"

	random "github.com/Pallinder/go-randomdata"
	"github.com/onedaycat/zamus/common"
)

type invokeReqBuilder struct {
	req *invoke.Request
}

func InvokeReq(fn string) *invokeReqBuilder {
	username := random.Noun()
	email := random.Email()
	ip := random.IpV4Address()
	return &invokeReqBuilder{
		req: &invoke.Request{
			Function: fn,
			Identity: &invoke.Identity{
				ID:     username,
				Email:  email,
				IPs:    []string{ip},
				Groups: []string{"user"},
				Pems:   make(map[string]string),
			},
		},
	}
}

func (b *invokeReqBuilder) Args(v interface{}) *invokeReqBuilder {
	b.req.WithArgs(v)

	return b
}

func (b *invokeReqBuilder) Permission(key string, permission string) *invokeReqBuilder {
	b.req.Identity.Pems[key] = permission

	return b
}

func (b *invokeReqBuilder) Warmer() *invokeReqBuilder {
	b.req = &invoke.Request{
		Warmer:     true,
		Concurency: 1,
	}

	return b
}

func (b *invokeReqBuilder) Build() *invoke.Request {
	return b.req
}

func (b *invokeReqBuilder) BuildJSON() []byte {
	data, err := common.MarshalJSON(b.req)
	if err != nil {
		panic(err)
	}

	return data
}
