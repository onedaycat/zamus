package kinesisstream

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/onedaycat/zamus/eventstore"

	"github.com/stretchr/testify/require"
)

//320
//248 22.5%
//224 30%

func TestJSONSize(t *testing.T) {
	var err error
	data := eventstore.EventMsg{
		EventID:     "bhh9lvkrtr33vbmh6djg1",
		EventType:   "domain.subdomain.aggregate.StockItemCreated",
		AggregateID: "bhh9lvkrtr33vbmh6djg",
		Seq:         10,
		Time:        1549966068,
	}

	data.Event, err = json.Marshal(map[string]interface{}{
		"id": "1",
	})
	require.NoError(t, err)
	bdata, err := json.Marshal(data)
	require.NoError(t, err)
	data1 := base64.StdEncoding.EncodeToString(bdata)

	fmt.Println(data1)
	fmt.Println(len(data1)) //320 30%
}

func TestSizeProto(t *testing.T) {
	var err error
	data := eventstore.EventMsg{
		EventID:     "bhh9lvkrtr33vbmh6djg1",
		EventType:   "domain.subdomain.aggregate.StockItemCreated",
		AggregateID: "bhh9lvkrtr33vbmh6djg",
		Seq:         10,
		Time:        1549966068,
	}

	data.Event, err = json.Marshal(map[string]interface{}{
		"id": "1",
	})
	require.NoError(t, err)
	bdata, err := data.Marshal()
	require.NoError(t, err)
	data1 := base64.StdEncoding.EncodeToString(bdata)

	fmt.Println(data1)
	fmt.Println(len(data1)) //224
}

func TestParseKinesisStreamEvent(t *testing.T) {
	var err error
	_ = []byte(`{
		"a": "a1",
		"b": "domain.aggregate",
		"s": 10,
		"e": "domain.aggregate.event",
		"x": 10001,
		"d": "{"id":"1"}"
	}`)

	data := eventstore.EventMsg{
		AggregateID: "a1",
		Seq:         10,
		EventType:   "domain.aggregate.event",
	}

	data.Event, err = json.Marshal(map[string]interface{}{
		"id": "1",
	})
	require.NoError(t, err)

	bdata, err := data.Marshal()
	require.NoError(t, err)
	data1 := base64.StdEncoding.EncodeToString(bdata)
	fmt.Println(data1)
	fmt.Println(len(data1))

	data.Seq = 11
	bdata, err = data.Marshal()
	require.NoError(t, err)
	data2 := base64.StdEncoding.EncodeToString(bdata)

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

	type pdata struct {
		ID string `json:"id"`
	}

	event := &KinesisStreamEvent{}
	err = json.Unmarshal(bpayload, event)
	require.NoError(t, err)
	require.Len(t, event.Records, 2)
	require.Equal(t, "domain.aggregate.event", event.Records[0].Kinesis.Data.EventMsg.EventType)
	require.Equal(t, int64(10), event.Records[0].Kinesis.Data.EventMsg.Seq)
	require.Equal(t, int64(11), event.Records[1].Kinesis.Data.EventMsg.Seq)

	pp := &pdata{}
	err = json.Unmarshal(event.Records[0].Kinesis.Data.EventMsg.Event, pp)
	require.NoError(t, err)
	fmt.Println(pp)
	require.Equal(t, &pdata{"1"}, pp)
}
