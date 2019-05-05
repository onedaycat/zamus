package sqs

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/testdata/domain"
	"github.com/stretchr/testify/require"
)

func TestParseSQSEvent(t *testing.T) {
	var err error

	evt := &domain.StockItemCreated{Id: "1"}
	evtByte, _ := event.MarshalEvent(evt)
	msg1 := &event.Msg{
		Id:        "a1",
		Event:     evtByte,
		EventType: proto.MessageName(evt),
		Time:      1,
	}

	msg1Byte, _ := event.MarshalMsg(msg1)
	data1 := base64.StdEncoding.EncodeToString(msg1Byte)

	msg2 := &event.Msg{
		Id:        "a1",
		Event:     evtByte,
		EventType: proto.MessageName(evt),
		Time:      2,
	}

	msg1Byte, _ = event.MarshalMsg(msg2)
	data2 := base64.StdEncoding.EncodeToString(msg1Byte)

	payload := fmt.Sprintf(`{
		"Records": [
			{
                "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                "body": "test",
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1545082649183",
                    "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                    "ApproximateFirstReceiveTimestamp": "1545082649185"
                },
                "messageAttributes": {
                    "msg": {
                        "Type": "Binary",
                        "Value": "%s"
                    },
                    "event": {
                        "Type": "String",
                        "Value": "testdata.stock.v1.StockItemCreated"
                    }
                },
                "md5OfBody": "098f6bcd4621d373cade4e832627b4f6",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                "awsRegion": "us-east-2"
            },
            {
                "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                "body": "test",
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1545082649183",
                    "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                    "ApproximateFirstReceiveTimestamp": "1545082649185"
                },
                "messageAttributes": {
                    "msg": {
                        "Type": "Binary",
                        "Value": "%s"
                    },
                    "event": {
                        "Type": "String",
                        "Value": "testdata.stock.v1.StockItemCreated"
                    }
                },
                "md5OfBody": "098f6bcd4621d373cade4e832627b4f6",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                "awsRegion": "us-east-2"
            }
		]
	}`, data1, data2)

	bpayload := []byte(payload)

	source := New()

	//fmt.Println(string(bpayload))

	req, err := source.GetRequest(context.Background(), bpayload)
	require.NoError(t, err)
	require.Len(t, req.Msgs, 2)
	require.Equal(t, msg1, req.Msgs[0])
	require.Equal(t, msg2, req.Msgs[1])

	pp := &domain.StockItemCreated{}
	err = event.UnmarshalEvent(req.Msgs[0].Event, pp)
	require.NoError(t, err)
	require.Equal(t, evt, pp)
}
