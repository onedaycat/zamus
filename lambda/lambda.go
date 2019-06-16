package lambda

import (
    "context"
    "encoding/json"
    "log"
    "net"
    "net/rpc"
    "os"

    jsoniter "github.com/json-iterator/go"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type Handler = func(ctx context.Context, payload json.RawMessage) (interface{}, error)
type Handle interface {
    Invoke(ctx context.Context, payload json.RawMessage) (interface{}, error)
}

type ld struct {
    handler Handler
}

func (l *ld) Invoke(ctx context.Context, payload json.RawMessage) (interface{}, error) {
    return l.handler(ctx, payload)
}

//noinspection GoUnusedExportedFunction
func Start(handler Handler) {
    StartHandler(&ld{handler})
}

func StartHandler(handle Handle) {
    port := os.Getenv("_LAMBDA_SERVER_PORT")
    lis, err := net.Listen("tcp", "localhost:"+port)
    if err != nil {
        log.Fatal(err)
    }
    function := new(Function)
    function.handle = handle
    err = rpc.Register(function)
    if err != nil {
        log.Fatal("failed to register handle function")
    }
    rpc.Accept(lis)
    log.Fatal("accept should not have returned")
}
