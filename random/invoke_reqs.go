package random

import (
    "github.com/onedaycat/zamus/internal/common"
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

func (b *invokeReqsBuilder) Add(fn string, inputs ...interface{}) *invokeReqsBuilder {
    for _, input := range inputs {
        b.reqs = append(b.reqs, InvokeReq(fn).Input(input).Build())
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
