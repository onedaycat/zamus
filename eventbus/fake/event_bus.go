package fake

import (
	"github.com/onedaycat/gocqrs"
)

type FakeEventBus struct{}

func FakecalEventBus() *FakeEventBus {
	return &FakeEventBus{}
}

func (k *FakeEventBus) Publish(events []*gocqrs.EventMessage) error {
	return nil
}
