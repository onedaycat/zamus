package random

import (
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/service"
)

type serviceReqsBuilder struct {
	reqs []*service.Request
	id   *service.Identity
}

func ServiceReqs() *serviceReqsBuilder {
	return &serviceReqsBuilder{
		reqs: make([]*service.Request, 0, 10),
	}
}

func (b *serviceReqsBuilder) Build() []*service.Request {
	b.setIdentity()
	return b.reqs
}

func (b *serviceReqsBuilder) Add(fn string, args ...interface{}) *serviceReqsBuilder {
	for _, arg := range args {
		b.reqs = append(b.reqs, ServiceReq(fn).Args(arg).Build())
	}

	return b
}

func (b *serviceReqsBuilder) Identity(id *service.Identity) *serviceReqsBuilder {
	b.id = id

	return b
}

func (b *serviceReqsBuilder) BuildJSON() []byte {
	b.setIdentity()
	data, err := common.MarshalJSON(b.reqs)
	if err != nil {
		panic(err)
	}

	return data
}

func (b *serviceReqsBuilder) setIdentity() {
	if b.id != nil {
		for i := range b.reqs {
			b.reqs[i].Identity = b.id
		}
	}
}
