package fake

import (
	"github.com/onedaycat/zamus"
)

type FakeEventBus struct{}

func FakecalEventBus() *FakeEventBus {
	return &FakeEventBus{}
}

func (k *FakeEventBus) Publish(events []*zamus.EventMessage) error {
	return nil
}
