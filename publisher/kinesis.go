package publisher

import (
    "context"

    "github.com/aws/aws-sdk-go/service/kinesis"
    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/reactor/dynamostream"
)

func (h *Handler) processKinesis(ctx context.Context, stream *dynamostream.EventSource) errors.Error {
    h.config.Kinesis.records = h.config.Kinesis.records[:0]
    if err := h.createKinesisRecords(ctx, stream); err != nil {
        return err
    }

    records := h.config.Kinesis.records

    if len(records) == 0 {
        return nil
    }

    for _, streamArn := range h.config.Kinesis.StreamARNs {
        streamArn := streamArn
        h.wgkin.Go(func() errors.Error {
            if err := h.publishKinesis(ctx, streamArn); err != nil {
                return err
            }

            return nil
        })
    }

    return h.wgkin.Wait()
}

func (h *Handler) createKinesisRecords(ctx context.Context, stream *dynamostream.EventSource) errors.Error {
    dyrecords := stream.Records
    kinrecs := h.config.Kinesis

    for i := 0; i < len(dyrecords); i++ {
        if dyrecords[i].EventName != dynamostream.EventInsert || dyrecords[i].DynamoDB.NewImage == nil {
            continue
        }

        data, _ := event.MarshalMsg(dyrecords[i].DynamoDB.NewImage.EventMsg)
        kinrecs.records = append(kinrecs.records, &kinesis.PutRecordsRequestEntry{
            Data:         data,
            PartitionKey: &dyrecords[i].DynamoDB.NewImage.EventMsg.AggID,
        })
    }

    return nil
}

func (h *Handler) publishKinesis(ctx context.Context, streamName string) errors.Error {
    input := &kinesis.PutRecordsInput{
        Records:    h.config.Kinesis.records,
        StreamName: &streamName,
    }

    out, err := h.config.Kinesis.Client.PutRecordsWithContext(ctx, input)

    if err != nil {
        return ErrUnablePublishKinesis.WithCaller().WithCause(err)
    }

    if out.FailedRecordCount != nil && *out.FailedRecordCount > 0 {
        return ErrUnablePublishKinesis.WithCaller().WithCause(errors.Simple("One or more events published failed"))
    }

    return nil
}
