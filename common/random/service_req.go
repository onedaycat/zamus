package random

import (
	"github.com/onedaycat/zamus/service"

	random "github.com/Pallinder/go-randomdata"
	"github.com/onedaycat/zamus/common"
)

type reqBuilder struct {
	req *service.Request
}

func ServiceReq(fn string) *reqBuilder {
	username := random.Noun()
	email := random.Email()
	ip := random.IpV4Address()
	return &reqBuilder{
		req: &service.Request{
			Function: fn,
			Identity: &service.Identity{
				ID:     username,
				Email:  email,
				IPs:    []string{ip},
				Groups: []string{"user"},
				Pems:   make(map[string]string),
			},
		},
	}
}

func (b *reqBuilder) Args(v interface{}) *reqBuilder {
	b.req.WithArgs(v)

	return b
}

func (b *reqBuilder) Permission(key string, permission string) *reqBuilder {
	b.req.Identity.Pems[key] = permission

	return b
}

func (b *reqBuilder) Warmer() *reqBuilder {
	b.req = &service.Request{
		Warmer:     true,
		Concurency: 1,
	}

	return b
}

func (b *reqBuilder) Build() *service.Request {
	return b.req
}

func (b *reqBuilder) BuildJSON() []byte {
	data, err := common.MarshalJSON(b.req)
	if err != nil {
		panic(err)
	}

	return data
}