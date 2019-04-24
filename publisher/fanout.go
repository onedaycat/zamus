package publisher

import (
    "context"

    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/invoke"
    "github.com/onedaycat/zamus/reactor/dynamostream"
)

func (h *Handler) processInvoke(ctx context.Context, stream *dynamostream.EventSource) errors.Error {
    if err := h.createInvokeRecords(ctx, stream); err != nil {
        Sentry(ctx, stream, err)
        return err
    }

    for _, ld := range h.config.Fanout.Lambdas {
        if len(ld.records.Msgs) == 0 {
            continue
        }

        ld := ld
        h.wginvoke.Go(func() errors.Error {
            if err := h.publishInvoke(ctx, ld.Fn, ld.records); err != nil {
                return err
            }

            return nil
        })
    }

    return h.wginvoke.Wait()
}

func (h *Handler) createInvokeRecords(ctx context.Context, stream *dynamostream.EventSource) errors.Error {
    dyrecords := stream.Records
    for _, ld := range h.config.Fanout.Lambdas {
        ld.records.Msgs = ld.records.Msgs[:0]
    }

    for i := 0; i < len(dyrecords); i++ {
        if dyrecords[i].EventName != dynamostream.EventInsert || dyrecords[i].DynamoDB.NewImage == nil {
            continue
        }

        indexs := h.config.Fanout.lambdaEvents[dyrecords[i].DynamoDB.NewImage.EventMsg.EventType]
        for _, index := range indexs {
            h.config.Fanout.Lambdas[index].records.Msgs = append(h.config.Fanout.Lambdas[index].records.Msgs, dyrecords[i].DynamoDB.NewImage.EventMsg)
        }
    }

    return nil
}

func (h *Handler) publishInvoke(ctx context.Context, fn string, msgList *event.MsgList) errors.Error {
    req := invoke.NewReactorRequest(fn).WithEventList(msgList)
    return h.config.Fanout.Invoker.InvokeReactor(ctx, req)
}
