package kinesisstream

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

func TestParseKinesisStreamEvent(t *testing.T) {
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
				"kinesis": {
					"kinesisSchemaVersion": "1.0",
					"partitionKey": "1",
					"sequenceNumber": "49590338271490256608559692538361571095921575989136588898",
					"data": "%s",
					"approximateArrivalTimestamp": 1545084650.987
				},
				"eventSource": "aws:kinesis",
				"eventVersion": "1.0",
				"eventID": "shardId-000000000006:49590338271490256608559692538361571095921575989136588898",
				"eventName": "aws:kinesis:record",
				"invokeIdentityArn": "arn:aws:iam::123456789012:role/lambda-role",
				"awsRegion": "us-east-2",
				"eventSourceARN": "arn:aws:kinesis:us-east-2:123456789012:stream/lambda-stream"
			},
			{
				"kinesis": {
					"kinesisSchemaVersion": "1.0",
					"partitionKey": "1",
					"sequenceNumber": "49590338271490256608559692540925702759324208523137515618",
					"data": "%s",
					"approximateArrivalTimestamp": 1545084711.166
				},
				"eventSource": "aws:kinesis",
				"eventVersion": "1.0",
				"eventID": "shardId-000000000006:49590338271490256608559692540925702759324208523137515618",
				"eventName": "aws:kinesis:record",
				"invokeIdentityArn": "arn:aws:iam::123456789012:role/lambda-role",
				"awsRegion": "us-east-2",
				"eventSourceARN": "arn:aws:kinesis:us-east-2:123456789012:stream/lambda-stream"
			}
		]
	}`, data1, data2)

	bpayload := []byte(payload)

	source := New()

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
