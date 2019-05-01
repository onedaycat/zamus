package dynamostream

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/testdata/domain"
	"github.com/stretchr/testify/require"
)

func TestParseDynamoDBStreamEvent(t *testing.T) {
	evt := &domain.StockItemCreated{Id: "1"}
	p, err := event.MarshalEvent(evt)
	p64 := base64.StdEncoding.EncodeToString(p)

	payload := fmt.Sprintf(`
	{
		"Records": [
			{
				"eventID": "7de3041dd709b024af6f29e4fa13d34c",
				"eventName": "INSERT",
				"eventVersion": "1.1",
				"eventSource": "aws:dynamodb",
				"awsRegion": "us-west-2",
				"dynamodb": {
					"ApproximateCreationDateTime": 1479499740,
					"Keys": {
						"Timestamp": {
							"S": "2016-11-18:12:09:36"
						},
						"Username": {
							"S": "John Doe"
						}
					},
					"NewImage": {
						"id": {
							"S": "a1"
						},
						"eventType": {
							"S": "testdata.stock.v1.StockItemCreated"
						},
						"event": {
							"B": "%s"
						}
					},
					"SequenceNumber": "13021600000000001596893679",
					"SizeBytes": 112,
					"StreamViewType": "NEW_IMAGE"
				},
				"eventSourceARN": "arn:aws:dynamodb:us-east-1:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104"
			},
			{
				"eventID":"3",
				"eventName":"REMOVE",
				"eventVersion":"1.0",
				"eventSource":"aws:dynamodb",
				"awsRegion":"us-east-1",
				"dynamodb":{
				   "Keys":{
					  "Id":{
						 "N":"101"
					  }
				   },
				   "NewImage": {
					"id": {
						"S": "a1"
					},
					"eventType": {
						"S": "testdata.stock.v1.StockItemCreated"
					},
					"event": {
						"B": "%s"
					}
				},
				   "SequenceNumber":"333",
				   "SizeBytes":38,
				   "StreamViewType":"NEW_IMAGES"
				},
				"eventSourceARN":"stream-ARN"
			 }
		]
	}`, p64, p64)

	source := New()
	req, err := source.GetRequest(context.Background(), []byte(payload))
	require.NoError(t, err)
	require.Len(t, req.Msgs, 1)
	require.Equal(t, "a1", req.Msgs[0].Id)
	require.Equal(t, "testdata.stock.v1.StockItemCreated", req.Msgs[0].EventType)

	pp := &domain.StockItemCreated{}
	err = req.Msgs[0].UnmarshalEvent(pp)
	require.NoError(t, err)
	require.Equal(t, evt, pp)
}
