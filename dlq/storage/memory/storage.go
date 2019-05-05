package memory

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/dlq"
    appErr "github.com/onedaycat/zamus/errors"
)

type dlqMemory struct {
    data map[string]*dlq.DLQMsg
}

func New() *dlqMemory {
    return &dlqMemory{
        data: make(map[string]*dlq.DLQMsg),
    }
}

func (d *dlqMemory) Truncate() {
    d.data = make(map[string]*dlq.DLQMsg)
}

func (d *dlqMemory) Save(ctx context.Context, dlqMsg *dlq.DLQMsg) errors.Error {
    d.data[dlqMsg.ID] = dlqMsg

    return nil
}

func (d *dlqMemory) Get(ctx context.Context, lambdaType dlq.LambdaType, id string) (*dlq.DLQMsg, errors.Error) {
    dqlMsg, ok := d.data[id]

    if !ok {
        return nil, appErr.ErrDLQMsgNotFound.WithCaller().WithInput(id)
    }

    return dqlMsg, nil
}
