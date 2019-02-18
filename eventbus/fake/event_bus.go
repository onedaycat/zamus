package fake

import (
	"github.com/onedaycat/zamus/eventstore"
)

type FakeEventBus struct{}

func FakecalEventBus() *FakeEventBus {
	return &FakeEventBus{}
}

func (k *FakeEventBus) Publish(events []*eventstore.EventMsg) error {
	return nil
}
