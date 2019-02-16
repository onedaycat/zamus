package zamus

//go:generate mockery -name=EventBus
type EventBus interface {
	Publish(events []*EventMessage) error
}
