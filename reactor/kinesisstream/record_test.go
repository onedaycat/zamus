package kinesisstream

import (
    "encoding/base64"
    "fmt"
    "testing"

    "github.com/gogo/protobuf/proto"
    "github.com/onedaycat/zamus/common"
    "github.com/onedaycat/zamus/eventstore"
    "github.com/onedaycat/zamus/testdata/domain"
    "github.com/stretchr/testify/require"
)

func TestParseKinesisStreamEvent(t *testing.T) {
    var err error

    evt := &domain.StockItemCreated{Id: "1"}
    evtByte, _ := common.MarshalEvent(evt)
    msg1 := &eventstore.EventMsg{
        AggregateID: "a1",
        Seq:         10,
        Event:       evtByte,
        EventType:   proto.MessageName(evt),
    }

    msg1Byte, _ := common.MarshalEventMsg(msg1)
    data1 := base64.StdEncoding.EncodeToString(msg1Byte)

    msg2 := &eventstore.EventMsg{
        AggregateID: "a1",
        Seq:         11,
        Event:       evtByte,
        EventType:   proto.MessageName(evt),
    }

    msg1Byte, _ = common.MarshalEventMsg(msg2)
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

    event := &KinesisStreamEvent{}
    err = common.UnmarshalJSON(bpayload, event)
    require.NoError(t, err)
    require.Len(t, event.Records, 2)
    require.Equal(t, msg1, event.Records[0].Kinesis.Data.EventMsg)
    require.Equal(t, msg2, event.Records[1].Kinesis.Data.EventMsg)

    pp := &domain.StockItemCreated{}
    err = common.UnmarshalEvent(event.Records[0].Kinesis.Data.EventMsg.Event, pp)
    require.NoError(t, err)
    require.Equal(t, evt, pp)
}
