package saga

import "testing"

func TestMiddleware(t *testing.T) {
	h := &testHandler{}
	New(h, nil, &Config{
		SentryDNS:   "test",
		EnableTrace: true,
	})
}
