package random

import (
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/invoke"
)

type invokeReqsBuilder struct {
	reqs []*invoke.Request
	id   *invoke.Identity
}

func InvokeReqs() *invokeReqsBuilder {
	return &invokeReqsBuilder{
		reqs: make([]*invoke.Request, 0, 10),
	}
}

func (b *invokeReqsBuilder) Build() []*invoke.Request {
	b.setIdentity()
	return b.reqs
}

func (b *invokeReqsBuilder) Add(fn string, args ...interface{}) *invokeReqsBuilder {
	for _, arg := range args {
		b.reqs = append(b.reqs, InvokeReq(fn).Args(arg).Build())
	}

	return b
}

func (b *invokeReqsBuilder) Identity(id *invoke.Identity) *invokeReqsBuilder {
	b.id = id

	return b
}

func (b *invokeReqsBuilder) BuildJSON() []byte {
	b.setIdentity()
	data, err := common.MarshalJSON(b.reqs)
	if err != nil {
		panic(err)
	}

	return data
}

func (b *invokeReqsBuilder) setIdentity() {
	if b.id != nil {
		for i := range b.reqs {
			b.reqs[i].Identity = b.id
		}
	}
}
