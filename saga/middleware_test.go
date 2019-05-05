package saga

import "testing"

func TestMiddleware(t *testing.T) {
    h := &testHandler{}
    New(nil, &Config{
        SentryDSN: "test",
        Handlers:  []SagaHandle{h},
    })
}
