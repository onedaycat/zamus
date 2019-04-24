package strategy

import (
    "context"
    "fmt"
    "runtime"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/errors/errgroup"
    "github.com/onedaycat/zamus/dql"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/reactor"
)

type shardinfoList []*shardinfo

func newShardInfoList(n int, c *shardStrategy) shardinfoList {
    shardinfoList := make([]*shardinfo, n)
    for i := range shardinfoList {
        shardinfoList[i] = &shardinfo{
            handlers: make([]*shardhandler, 0),
            pk:       common.NewSetListFromList(make([]string, 0, 100)),
            c:        c,
        }
    }

    return shardinfoList
}

func (s shardinfoList) GetPK(key string) (int, bool) {
    for i := range s {
        if s[i].pk.Has(key) {
            return i, true
        }
    }

    return 0, false
}

func (s shardinfoList) AddPK(shard int, key string) {
    s[shard].pk.Set(key)
}

func (s shardinfoList) Clear() {
    for i := range s {
        s[i].Clear()
    }
}

type shardinfo struct {
    handlers    []*shardhandler
    pk          common.SetList
    eventLength int
    ctx         context.Context
    c           *shardStrategy
}

func (s *shardinfo) AddEventMsg(msg *event.Msg) {
    for _, handler := range s.handlers {
        if handler.AddEventMsg(msg) {
            s.eventLength++
        }
    }
}

func (s *shardinfo) AddHandler(handler reactor.EventHandler, sl common.SetList) {
    s.handlers = append(s.handlers, &shardhandler{
        Handler:      handler,
        FilterEvents: sl,
        EventMsgs:    make([]event.Msgs, 100),
        pk:           make([]string, 0, 100),
    })
}

func (s *shardinfo) Clear() {
    s.pk.Clear()
    for _, handler := range s.handlers {
        handler.pk = handler.pk[:0]
        for i := range handler.EventMsgs {
            handler.EventMsgs[i] = handler.EventMsgs[i][:0]
        }
    }
}

type shardhandler struct {
    Handler      reactor.EventHandler
    FilterEvents common.SetList
    EventMsgs    []event.Msgs
    pk           []string
}

func (s *shardhandler) GetPK(pk string) (int, bool) {
    for i := range s.pk {
        if s.pk[i] == pk {
            return i, true
        }
    }

    return 0, false
}

func (s *shardhandler) AddPK(pk string) int {
    for i := range s.pk {
        if s.pk[i] == pk {
            return i
        }
    }

    s.pk = append(s.pk, pk)
    return len(s.pk) - 1
}

func (s *shardhandler) AddEventMsg(msg *event.Msg) bool {
    if s.FilterEvents == nil {
        index, ok := s.GetPK(msg.AggID)
        if !ok {
            index = s.AddPK(msg.AggID)
        }
        s.EventMsgs[index] = append(s.EventMsgs[index], msg)
        return true
    }

    if s.FilterEvents.Has(msg.EventType) {
        index, ok := s.GetPK(msg.AggID)
        if !ok {
            index = s.AddPK(msg.AggID)
        }
        s.EventMsgs[index] = append(s.EventMsgs[index], msg)
        return true
    }

    return false
}

type shardStrategy struct {
    wg            errgroup.Group
    nShard        int
    errorHandlers []reactor.ErrorHandler
    shardinfoList shardinfoList
    eventTypes    common.Set
    preHandlers   []reactor.EventHandler
    postHandlers  []reactor.EventHandler
    dql           dql.DQL
}

func NewShard(numShard ...int) *shardStrategy {
    var shard int
    if len(numShard) == 0 {
        shard = runtime.NumCPU()
    } else {
        shard = numShard[0]
    }

    s := &shardStrategy{
        eventTypes: common.NewSet(),
        nShard:     shard,
    }

    s.shardinfoList = newShardInfoList(shard, s)

    return s
}

func (c *shardStrategy) ErrorHandlers(handlers ...reactor.ErrorHandler) {
    c.errorHandlers = append(c.errorHandlers, handlers...)
}

func (c *shardStrategy) SetDQL(dql dql.DQL) {
    c.dql = dql
}

func (c *shardStrategy) PreHandlers(handlers ...reactor.EventHandler) {
    c.preHandlers = append(c.preHandlers, handlers...)
}

func (c *shardStrategy) PostHandlers(handlers ...reactor.EventHandler) {
    c.postHandlers = append(c.postHandlers, handlers...)
}

func (c *shardStrategy) RegisterHandler(handler reactor.EventHandler, filterEvents []string) {
    var sl common.SetList
    if filterEvents != nil {
        sl = common.NewSetListFromList(filterEvents)
        c.eventTypes.SetMany(filterEvents)
    }

    for i := range c.shardinfoList {
        c.shardinfoList[i].AddHandler(handler, sl)
    }
}

func (c *shardStrategy) Process(ctx context.Context, msgs event.Msgs) errors.Error {
    var pos int
    var ok bool

    c.shardinfoList.Clear()

    if len(c.eventTypes) > 0 {
        for i, msg := range msgs {
            if !c.eventTypes.Has(msg.EventType) {
                continue
            }

            pos, ok = c.shardinfoList.GetPK(msg.AggID)
            if !ok {
                pos = i % c.nShard
                c.shardinfoList.AddPK(pos, msg.AggID)
            }

            c.shardinfoList[pos].AddEventMsg(msg)
        }
    } else {
        for i, msg := range msgs {
            pos, ok = c.shardinfoList.GetPK(msg.AggID)
            if !ok {
                pos = i % c.nShard
                c.shardinfoList.AddPK(pos, msg.AggID)
            }

            c.shardinfoList[pos].AddEventMsg(msg)
        }
    }
DQLRetry:
    for _, shard := range c.shardinfoList {
        if shard.eventLength == 0 {
            continue
        }

        shard.ctx = ctx
        c.wg.Go(shard.handleShard)
    }

    if err := c.wg.Wait(); err != nil {
        if c.dql != nil {
            if ok := c.dql.Retry(); ok {
                goto DQLRetry
            }

            msgs := make(event.Msgs, 0, len(msgs))
            if len(c.eventTypes) > 0 {
                for _, msg := range msgs {
                    if !c.eventTypes.Has(msg.EventType) {
                        continue
                    }

                    msgs = append(msgs, msg)
                }
            } else {
                for _, msg := range msgs {
                    msgs = append(msgs, msg)
                }
            }

            msgList := &event.MsgList{
                Msgs: msgs,
            }
            msgListByte, _ := event.MarshalMsg(msgList)

            return c.dql.Save(ctx, msgListByte)
        }

        return err
    }

    return nil
}

func (c *shardStrategy) doPreHandlers(ctx context.Context, msgs event.Msgs) (err errors.Error) {
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

func (c *shardStrategy) doPostHandler(ctx context.Context, msgs event.Msgs) (err errors.Error) {
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

func (c *shardStrategy) doHandlers(ctx context.Context, msgs event.Msgs, handler reactor.EventHandler) (err errors.Error) {
    defer c.recover(ctx, msgs, &err)
    if err = handler(ctx, msgs); err != nil {
        if c.dql != nil {
            c.dql.AddError(err)
        }
        for _, errhandler := range c.errorHandlers {
            errhandler(ctx, msgs, err)
        }
        return err
    }

    return nil
}

func (s *shardinfo) handleShard() (err errors.Error) {
    return s.c.handle(s.ctx, s)
}

func (c *shardStrategy) handle(ctx context.Context, shard *shardinfo) errors.Error {
    var err errors.Error

    for i := range shard.handlers {
        if len(shard.handlers[i].EventMsgs) == 0 {
            continue
        }

        for _, msgs := range shard.handlers[i].EventMsgs {
            if len(msgs) == 0 {
                continue
            }

            if err = shard.c.doPreHandlers(ctx, msgs); err != nil {
                return err
            }

            if err = c.doHandlers(ctx, msgs, shard.handlers[i].Handler); err != nil {
                return err
            }

            if err = shard.c.doPostHandler(ctx, msgs); err != nil {
                return err
            }
        }
    }

    return nil
}

func (c *shardStrategy) recover(ctx context.Context, msgs event.Msgs, err *errors.Error) {
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
