package publisher

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/invoke"
)

type ReactorConfig struct {
	Fn           string
	FilterEvents []string
	records      *event.MsgList
	eventTypes   map[string]struct{}
	isAll        bool
	client       invoke.Invoker
	ctx          context.Context
}

func (c *ReactorConfig) init() {
	if len(c.FilterEvents) > 0 {
		c.eventTypes = make(map[string]struct{})
		for _, eventType := range c.FilterEvents {
			c.eventTypes[eventType] = struct{}{}
		}
	} else {
		c.isAll = true
	}
	c.records = &event.MsgList{
		Msgs: make(event.Msgs, 0, 100),
	}
}

func (c *ReactorConfig) filter(msg *event.Msg) {
	if c.isAll {
		c.records.Msgs = append(c.records.Msgs, msg)
	} else {
		_, ok := c.eventTypes[msg.EventType]
		if ok {
			c.records.Msgs = append(c.records.Msgs, msg)
		}
	}
}

func (c *ReactorConfig) clear() {
	c.records.Msgs = c.records.Msgs[:0]
}

func (c *ReactorConfig) hasEvents() bool {
	return len(c.records.Msgs) > 0 || c.isAll
}

func (c *ReactorConfig) setContext(ctx context.Context) {
	c.ctx = ctx
}

func (c *ReactorConfig) publish() errors.Error {
	req := invoke.NewReactorRequest(c.Fn).WithEventList(c.records)
	_ = c.client.InvokeReactor(c.ctx, req)

	return nil
}

//import (
//    "context"
//
//    "github.com/onedaycat/errors"
//    "github.com/onedaycat/zamus/event"
//    "github.com/onedaycat/zamus/invoke"
//    "github.com/onedaycat/zamus/reactor/dynamostream"
//)
//
//func (h *Handler) processLambda(ctx context.Context, stream *dynamostream.EventSource) errors.Error {
//    if err := h.createLambdaRecords(ctx, stream); err != nil {
//        Sentry(ctx, stream, err)
//        return err
//    }
//
//    for _, ld := range h.config.Lambda {
//        if len(ld.records.Msgs) == 0 {
//            continue
//        }
//
//        ld := ld
//        h.wginvoke.Go(func() errors.Error {
//            if ld.Saga {
//                return h.publishSaga(ctx, ld.Fn, ld.records)
//            }
//
//            return h.publishReactor(ctx, ld.Fn, ld.records)
//        })
//    }
//
//    return h.wginvoke.Wait()
//}
//
//func (h *Handler) createLambdaRecords(ctx context.Context, stream *dynamostream.EventSource) errors.Error {
//    dyrecords := stream.Records
//    for _, ld := range h.config.Lambda {
//        ld.records.Msgs = ld.records.Msgs[:0]
//    }
//
//    for i := 0; i < len(dyrecords); i++ {
//        if dyrecords[i].EventName != dynamostream.EventInsert || dyrecords[i].DynamoDB.NewImage == nil {
//            continue
//        }
//
//        indexs := h.config.lambdaEvents[dyrecords[i].DynamoDB.NewImage.EventMsg.EventType]
//        for _, index := range indexs {
//            h.config.Lambda[index].records.Msgs = append(h.config.Lambda[index].records.Msgs, dyrecords[i].DynamoDB.NewImage.EventMsg)
//        }
//    }
//
//    return nil
//}
//
//func (h *Handler) publishReactor(ctx context.Context, fn string, msgList *event.MsgList) errors.Error {
//    req := invoke.NewReactorRequest(fn).WithEventList(msgList)
//    return h.config.Invoker.InvokeReactor(ctx, req)
//}
//
//func (h *Handler) publishSaga(ctx context.Context, fn string, msgList *event.MsgList) errors.Error {
//    n := len(msgList.Msgs)
//    sagaShard := make(map[string]event.Msgs)
//    for _, msg := range msgList.Msgs {
//        if sagaShard[msg.AggID] == nil {
//            sagaShard[msg.AggID] = make(event.Msgs, 0, n)
//        }
//
//        sagaShard[msg.AggID] = append(sagaShard[msg.AggID], msg)
//    }
//
//    for _, msgs := range sagaShard {
//        msgs := msgs
//        h.wginvokeSaga.Go(func() errors.Error {
//            for _, msg := range msgs {
//                req := invoke.NewSagaRequest(fn).WithInput(msg)
//                _ = h.config.Invoker.InvokeSaga(ctx, req, nil)
//            }
//
//            return nil
//        })
//    }
//
//    return h.wginvokeSaga.Wait()
//}
