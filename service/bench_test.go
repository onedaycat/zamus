package service_test

import (
    "context"
    "testing"

    "github.com/onedaycat/zamus/common/random"
)

func BenchmarkHandler(b *testing.B) {
    s := setupHandlerSuite()
    s.WithHandler("f1", MODE_NORMAL, s.result)

    payload := random.ServiceReq("f1").BuildJSON()

    b.ReportAllocs()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        //noinspection GoUnhandledErrorResult
        s.h.Invoke(context.Background(), payload)
    }
}
