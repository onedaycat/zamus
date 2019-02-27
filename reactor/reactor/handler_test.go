package reactor

// import (
// 	"context"
// 	"encoding/base64"
// 	"encoding/json"
// 	"fmt"
// 	"testing"

// 	"github.com/onedaycat/errors"
// 	"github.com/onedaycat/zamus/eventstore"
// 	"github.com/onedaycat/zamus/lambdastream/kinesisstream"
// 	"github.com/stretchr/testify/require"
// )

// func testEventData() (*kinesisstream.KinesisStreamEvent, error) {
// 	var err error
// 	data := eventstore.EventMsg{
// 		AggregateID:   "a1",
// 		AggregateType: "domain.aggregate",
// 		Seq:           10,
// 		EventType:     "registerForm.registerFormCreated",
// 		TimeSeq:       10001,
// 	}

// 	data.Data, err = json.Marshal(map[string]interface{}{
// 		"id": "1",
// 	})
// 	if err != nil {
// 		return nil, err
// 	}

// 	bdata, err := data.Marshal()
// 	if err != nil {
// 		return nil, err
// 	}

// 	data1 := base64.StdEncoding.EncodeToString(bdata)
// 	data.Seq = 11
// 	bdata, err = data.Marshal()
// 	if err != nil {
// 		return nil, err
// 	}

// 	data2 := base64.StdEncoding.EncodeToString(bdata)

// 	payload := fmt.Sprintf(`{
// 		"Records": [
// 			{
// 				"kinesis": {
// 					"kinesisSchemaVersion": "1.0",
// 					"partitionKey": "1",
// 					"sequenceNumber": "49590338271490256608559692538361571095921575989136588898",
// 					"data": "%s",
// 					"approximateArrivalTimestamp": 1545084650.987
// 				},
// 				"eventSource": "aws:kinesis",
// 				"eventVersion": "1.0",
// 				"eventID": "shardId-000000000006:49590338271490256608559692538361571095921575989136588898",
// 				"eventName": "aws:kinesis:record",
// 				"invokeIdentityArn": "arn:aws:iam::123456789012:role/lambda-role",
// 				"awsRegion": "us-east-2",
// 				"eventSourceARN": "arn:aws:kinesis:us-east-2:123456789012:stream/lambda-stream"
// 			},
// 			{
// 				"kinesis": {
// 					"kinesisSchemaVersion": "1.0",
// 					"partitionKey": "1",
// 					"sequenceNumber": "49590338271490256608559692540925702759324208523137515618",
// 					"data": "%s",
// 					"approximateArrivalTimestamp": 1545084711.166
// 				},
// 				"eventSource": "aws:kinesis",
// 				"eventVersion": "1.0",
// 				"eventID": "shardId-000000000006:49590338271490256608559692540925702759324208523137515618",
// 				"eventName": "aws:kinesis:record",
// 				"invokeIdentityArn": "arn:aws:iam::123456789012:role/lambda-role",
// 				"awsRegion": "us-east-2",
// 				"eventSourceARN": "arn:aws:kinesis:us-east-2:123456789012:stream/lambda-stream"
// 			}
// 		]
// 	}`, data1, data2)

// 	bpayload := []byte(payload)

// 	event := &kinesisstream.KinesisStreamEvent{}
// 	err = json.Unmarshal(bpayload, event)

// 	return event, err
// }
// func TestEventHandler(t *testing.T) {
// 	events, _ := testEventData()
// 	checkFunc := false
// 	lenData := 0

// 	kf := func(msgs kinesisstream.EventMsgs) (*kinesisstream.EventMsg, error) {
// 		checkFunc = true
// 		lenData = len(msgs)
// 		return nil, nil
// 	}

// 	h := NewHandler()
// 	h.RegisterEvent("registerForm.registerFormCreated", kf)
// 	h.handler(context.Background(), events)

// 	require.True(t, checkFunc)
// 	require.Equal(t, 2, lenData)
// }

// func TestPreAndPostEventHandler(t *testing.T) {
// 	events, _ := testEventData()
// 	checkPreFunc := false
// 	checkPostFunc := false
// 	checkFunc := false

// 	f := func(msgs kinesisstream.EventMsgs) (*kinesisstream.EventMsg, error) {
// 		checkFunc = true
// 		require.Equal(t, 2, len(msgs))

// 		return nil, nil
// 	}

// 	preFunc := func(msgs kinesisstream.EventMsgs) (*kinesisstream.EventMsg, error) {
// 		checkPreFunc = true
// 		require.Equal(t, 2, len(msgs))

// 		return nil, nil
// 	}

// 	postFunc := func(msgs kinesisstream.EventMsgs) (*kinesisstream.EventMsg, error) {
// 		checkPostFunc = true
// 		require.Equal(t, 2, len(msgs))

// 		return nil, nil
// 	}

// 	h := NewHandler()
// 	h.PreHandler(preFunc)
// 	h.PostHandler(postFunc)
// 	h.RegisterEvent("registerForm.registerFormCreated", f)

// 	h.handler(context.Background(), events)

// 	require.True(t, checkFunc)
// 	require.True(t, checkPreFunc)
// 	require.True(t, checkPostFunc)
// }

// func TestErrorEventHandler(t *testing.T) {
// 	events, _ := testEventData()
// 	checkErrorFunc := false
// 	checkFunc := false
// 	eventMsg := &kinesisstream.EventMsg{}

// 	f := func(msgs kinesisstream.EventMsgs) (*kinesisstream.EventMsg, error) {
// 		eventMsg = msgs[0]
// 		checkFunc = true
// 		require.Equal(t, 2, len(msgs))

// 		return eventMsg, errors.InternalError("10000", "Test Error")
// 	}

// 	errFunc := func(msg *kinesisstream.EventMsg, err error) {
// 		require.Error(t, err)
// 		require.Equal(t, eventMsg, msg)

// 		checkErrorFunc = true
// 	}

// 	h := NewHandler()
// 	h.ErrorHandler(errFunc)
// 	h.RegisterEvent("registerForm.registerFormCreated", f)

// 	h.handler(context.Background(), events)

// 	require.True(t, checkFunc)
// 	require.True(t, checkErrorFunc)
// }
