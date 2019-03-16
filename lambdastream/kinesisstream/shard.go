package kinesisstream

import (
	"context"
	"fmt"
	"runtime"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/errors/errgroup"
	"github.com/onedaycat/zamus/common"
	"github.com/onedaycat/zamus/dql"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/tracer"
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

func (s *shardinfo) AddEventMsg(msg *EventMsg) {
	for _, handler := range s.handlers {
		if handler.AddEventMsg(msg) {
			s.eventLength++
		}
	}
}

func (s *shardinfo) AddHandler(handler EventMessagesHandler, sl common.SetList) {
	s.handlers = append(s.handlers, &shardhandler{
		Handler:      handler,
		FilterEvents: sl,
		EventMsgs:    make(EventMsgs, 0, 100),
	})
}

func (s *shardinfo) Clear() {
	s.pk.Clear()
	for _, handler := range s.handlers {
		handler.EventMsgs = handler.EventMsgs[:0]
	}
}

type shardhandler struct {
	Handler      EventMessagesHandler
	FilterEvents common.SetList
	EventMsgs    EventMsgs
}

func (s *shardhandler) AddEventMsg(msg *EventMsg) bool {
	if s.FilterEvents == nil {
		s.EventMsgs = append(s.EventMsgs, msg)
		return true
	}

	if s.FilterEvents.Has(msg.EventType) {
		s.EventMsgs = append(s.EventMsgs, msg)
		return true
	}

	return false
}

type shardStrategy struct {
	wg            errgroup.Group
	nShard        int
	errorHandlers []EventMessagesErrorHandler
	shardinfoList shardinfoList
	eventTypes    common.Set
	preHandlers   []EventMessagesHandler
	postHandlers  []EventMessagesHandler
	dql           dql.DQL
}

func NewShardStrategy(numShard ...int) KinesisHandlerStrategy {
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

func (c *shardStrategy) ErrorHandlers(handlers ...EventMessagesErrorHandler) {
	c.errorHandlers = handlers
}

func (c *shardStrategy) SetDQL(dql dql.DQL) {
	c.dql = dql
}

func (c *shardStrategy) PreHandlers(handlers ...EventMessagesHandler) {
	c.preHandlers = handlers
}

func (c *shardStrategy) PostHandlers(handlers ...EventMessagesHandler) {
	c.postHandlers = handlers
}

func (c *shardStrategy) RegisterHandler(handler EventMessagesHandler, filterEvents []string) {
	var sl common.SetList
	if filterEvents != nil {
		sl = common.NewSetListFromList(filterEvents)
		c.eventTypes.SetMany(filterEvents)
	}

	for i := range c.shardinfoList {
		c.shardinfoList[i].AddHandler(handler, sl)
	}
}

func (c *shardStrategy) Process(ctx context.Context, records Records) errors.Error {
	var eventType string
	var pk string
	var pos int
	var ok bool

	c.shardinfoList.Clear()

	if len(c.eventTypes) > 0 {
		for i, record := range records {
			eventType = record.Kinesis.Data.EventMsg.EventType
			if !c.eventTypes.Has(eventType) {
				continue
			}

			pk = record.Kinesis.PartitionKey
			pos, ok = c.shardinfoList.GetPK(pk)
			if !ok {
				pos = i % c.nShard
				c.shardinfoList.AddPK(pos, pk)
			}

			c.shardinfoList[pos].AddEventMsg(record.Kinesis.Data.EventMsg)
		}
	} else {
		for i, record := range records {
			pk = record.Kinesis.PartitionKey
			pos, ok = c.shardinfoList.GetPK(pk)
			if !ok {
				pos = i % c.nShard
				c.shardinfoList.AddPK(pos, pk)
			}

			c.shardinfoList[pos].AddEventMsg(record.Kinesis.Data.EventMsg)
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

			msgs := make(EventMsgs, len(records))
			for i, record := range records {
				eventType = record.Kinesis.Data.EventMsg.EventType
				if !c.eventTypes.Has(eventType) {
					continue
				}
				msgs[i] = record.Kinesis.Data.EventMsg
			}

			msgList := eventstore.EventMsgList{
				EventMsgs: msgs,
			}
			msgListByte, _ := msgList.Marshal()

			return c.dql.Save(ctx, msgListByte)
		}

		return err
	}

	return nil
}

func (c *shardStrategy) doPreHandlers(ctx context.Context, msgs EventMsgs) (err errors.Error) {
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

func (c *shardStrategy) doPostHandler(ctx context.Context, msgs EventMsgs) (err errors.Error) {
	defer c.recover(ctx, msgs, &err)
	for _, ph := range c.postHandlers {
		if err = ph(ctx, msgs); err != nil {
			if c.dql != nil {
				c.dql.AddError(err)
			}
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, err)
			}
		}

		return err
	}

	return
}

func (c *shardStrategy) doHandlers(ctx context.Context, msgs EventMsgs, handler EventMessagesHandler) (err errors.Error) {
	defer c.recover(ctx, msgs, &err)
	if err = handler(ctx, msgs); err != nil {
		tracer.AddError(ctx, err)
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
		if err = shard.c.doPreHandlers(ctx, nil); err != nil {
			return err
		}

		if len(shard.handlers[i].EventMsgs) == 0 {
			continue
		}

		if err = c.doHandlers(ctx, shard.handlers[i].EventMsgs, shard.handlers[i].Handler); err != nil {
			return err
		}

		if err = shard.c.doPostHandler(ctx, nil); err != nil {
			return err
		}
	}

	return nil
}

func (c *shardStrategy) recover(ctx context.Context, msgs EventMsgs, err *errors.Error) {
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
			tracer.AddError(ctx, *err)
		default:
			*err = appErr.ErrPanic.WithCauseMessage(fmt.Sprintf("%v\n", cause)).WithCaller().WithInput(msgs)
			if c.dql != nil {
				c.dql.AddError(*err)
			}
			for _, errhandler := range c.errorHandlers {
				errhandler(ctx, msgs, *err)
			}
			tracer.AddError(ctx, *err)
		}
	}
}
