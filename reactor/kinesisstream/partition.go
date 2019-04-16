package kinesisstream

import (
    "context"
    "fmt"
    "sync"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/errors/errgroup"

    "github.com/onedaycat/zamus/common"
    "github.com/onedaycat/zamus/dql"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/eventstore"
)

type partitionStrategy struct {
    pkPool        sync.Pool
    errorHandlers []EventMessagesErrorHandler
    handlers      []*handlerInfo
    eventTypes    common.Set
    preHandlers   []EventMessagesHandler
    postHandlers  []EventMessagesHandler
    dql           dql.DQL
}

func NewPartitionStrategy() KinesisHandlerStrategy {
    ps := &partitionStrategy{
        eventTypes: common.NewSet(),
        handlers:   make([]*handlerInfo, 0, 10),
    }

    ps.pkPool = sync.Pool{
        New: func() interface{} {
            return make(map[string]EventMsgs, 100)
        },
    }

    return ps
}

func (c *partitionStrategy) ErrorHandlers(handlers ...EventMessagesErrorHandler) {
    c.errorHandlers = append(c.errorHandlers, handlers...)
}

func (c *partitionStrategy) SetDQL(dql dql.DQL) {
    c.dql = dql
}

func (c *partitionStrategy) PreHandlers(handlers ...EventMessagesHandler) {
    c.preHandlers = append(c.preHandlers, handlers...)
}

func (c *partitionStrategy) PostHandlers(handlers ...EventMessagesHandler) {
    c.postHandlers = append(c.postHandlers, handlers...)
}

func (c *partitionStrategy) RegisterHandler(handler EventMessagesHandler, filterEvents []string) {
    if filterEvents == nil {
        c.handlers = append(c.handlers, &handlerInfo{
            Handler:      handler,
            FilterEvents: nil,
        })
    } else {
        c.handlers = append(c.handlers, &handlerInfo{
            Handler:      handler,
            FilterEvents: common.NewSetFromList(filterEvents),
        })
        c.eventTypes.SetMany(filterEvents)
    }
}

func (c *partitionStrategy) Process(ctx context.Context, records Records) errors.Error {
    var eventType string
    var pk string
    partitions := c.pkPool.Get().(map[string]EventMsgs)

    defer func() {
        for key := range partitions {
            delete(partitions, key)
        }
        c.pkPool.Put(partitions)
    }()

    if len(c.eventTypes) > 0 {
        for _, record := range records {
            eventType = record.Kinesis.Data.EventMsg.EventType
            if !c.eventTypes.Has(eventType) {
                continue
            }

            pk = record.Kinesis.PartitionKey

            partitions[pk] = append(partitions[pk], record.Kinesis.Data.EventMsg)
        }
    } else {
        for _, record := range records {
            pk = record.Kinesis.PartitionKey

            partitions[pk] = append(partitions[pk], record.Kinesis.Data.EventMsg)
        }
    }

DQLRetry:

    wg := errgroup.Group{}

    for _, ghs := range partitions {
        ghs := ghs
        if len(ghs) == 0 {
            continue
        }

        wg.Go(func() errors.Error {
            return c.handle(ctx, ghs)
        })
    }

    if err := wg.Wait(); err != nil {
        if c.dql != nil {
            if ok := c.dql.Retry(); ok {
                goto DQLRetry
            }

            msgs := make(EventMsgs, 0, len(records))
            if len(c.eventTypes) > 0 {
                for _, record := range records {
                    eventType = record.Kinesis.Data.EventMsg.EventType
                    if !c.eventTypes.Has(eventType) {
                        continue
                    }

                    msgs = append(msgs, record.Kinesis.Data.EventMsg)
                }
            } else {
                for _, record := range records {
                    msgs = append(msgs, record.Kinesis.Data.EventMsg)
                }
            }

            msgList := &eventstore.EventMsgList{
                EventMsgs: msgs,
            }
            msgListByte, _ := common.MarshalEventMsg(msgList)

            return c.dql.Save(ctx, msgListByte)
        }

        return err
    }

    return nil
}

func (c *partitionStrategy) doPreHandlers(ctx context.Context, msgs EventMsgs) (err errors.Error) {
    defer c.recover(ctx, msgs, &err)
    for _, ph := range c.preHandlers {
        if err = ph(ctx, msgs); err != nil {
            if c.dql != nil {
                c.dql.AddError(err)
            }
            for _, errhandler := range c.errorHandlers {
                errhandler(ctx, msgs, err)
            }

            return err
        }
    }

    return
}

func (c *partitionStrategy) doPostHandler(ctx context.Context, msgs EventMsgs) (err errors.Error) {
    defer c.recover(ctx, msgs, &err)
    for _, ph := range c.postHandlers {
        if err = ph(ctx, msgs); err != nil {
            if c.dql != nil {
                c.dql.AddError(err)
            }
            for _, errhandler := range c.errorHandlers {
                errhandler(ctx, msgs, err)
            }

            return err
        }
    }

    return
}

func (c *partitionStrategy) filterEvents(info *handlerInfo, msgs EventMsgs) EventMsgs {
    if info.FilterEvents.IsEmpty() {
        return msgs
    }

    var ok bool
    fillter := make(EventMsgs, 0, len(msgs))
    for _, msg := range msgs {
        if ok = info.FilterEvents.Has(msg.EventType); ok {
            fillter = append(fillter, msg)
        }
    }

    return fillter
}

func (c *partitionStrategy) doHandlers(ctx context.Context, msgs EventMsgs) (err errors.Error) {
    wg := errgroup.Group{}
    for _, handlerinfo := range c.handlers {
        handlerinfo := handlerinfo
        wg.Go(func() (aerr errors.Error) {
            defer c.recover(ctx, msgs, &aerr)
            newmsgs := c.filterEvents(handlerinfo, msgs)
            if len(newmsgs) == 0 {
                return nil
            }

            if aerr = handlerinfo.Handler(ctx, newmsgs); aerr != nil {
                if c.dql != nil {
                    c.dql.AddError(aerr)
                }
                for _, errhandler := range c.errorHandlers {
                    errhandler(ctx, newmsgs, aerr)
                }
                return aerr
            }

            return
        })
    }

    return wg.Wait()
}

func (c *partitionStrategy) handle(ctx context.Context, msgs EventMsgs) (err errors.Error) {
    if len(msgs) == 0 {
        return nil
    }

    if err = c.doPreHandlers(ctx, msgs); err != nil {
        return err
    }

    if err = c.doHandlers(ctx, msgs); err != nil {
        return err
    }

    if err = c.doPostHandler(ctx, msgs); err != nil {
        return err
    }

    return
}

func (c *partitionStrategy) recover(ctx context.Context, msgs EventMsgs, err *errors.Error) {
    if r := recover(); r != nil {
        switch cause := r.(type) {
        case error:
            *err = appErr.ErrPanic.WithCause(cause).WithCaller().WithInput(msgs)
            if c.dql != nil {
                c.dql.AddError(*err)
            }
            for _, errhandler := range c.errorHandlers {
                errhandler(ctx, msgs, *err)
            }
        default:
            *err = appErr.ErrPanic.WithCauseMessage(fmt.Sprintf("%v\n", cause)).WithCaller().WithInput(msgs)
            if c.dql != nil {
                c.dql.AddError(*err)
            }
            for _, errhandler := range c.errorHandlers {
                errhandler(ctx, msgs, *err)
            }
        }
    }
}
