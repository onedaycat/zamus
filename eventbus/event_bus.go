package eventbus

import "github.com/onedaycat/zamus/eventstore"

//go:generate mockery -name=EventBus
type EventBus interface {
	Publish(events []*eventstore.EventMsg) error
}
