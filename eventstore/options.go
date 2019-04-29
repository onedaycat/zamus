package eventstore

type Option func(o *option)

type option struct {
	allowSanpshot   bool
	snapshotVersion int
}

func WithSnapshot(version int) Option {
	return func(o *option) {
		o.allowSanpshot = true
		o.snapshotVersion = version
	}
}
