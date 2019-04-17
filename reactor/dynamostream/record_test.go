package dynamostream

import (
    "encoding/base64"
    "fmt"
    "sort"
    "testing"

    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
    "github.com/onedaycat/zamus/testdata/domain"
    "github.com/stretchr/testify/require"
)

func TestParseDynamoDBStreamEvent(t *testing.T) {
    evt := &domain.StockItemCreated{Id: "1"}
    p, err := event.MarshalEvent(evt)
    require.NoError(t, err)
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
						"aggID": {
							"S": "a1"
						},
						"eventType": {
							"S": "domain.aggregate.dyevt"
						},
						"seq": {
							"N": "10002"
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
					"aggID": {
						"S": "a1"
					},
					"eventType": {
						"S": "domain.aggregate.dyevt"
					},
					"seq": {
						"N": "10001"
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

    dyevt := &DynamoDBStreamEvent{}
    err = common.UnmarshalJSON([]byte(payload), dyevt)
    sort.Sort(dyevt.Records)
    require.NoError(t, err)
    require.Len(t, dyevt.Records, 2)
    require.Equal(t, eventRemove, dyevt.Records[0].EventName)
    require.Equal(t, int64(10001), dyevt.Records[0].DynamoDB.NewImage.EventMsg.Seq)
    require.Equal(t, EventInsert, dyevt.Records[1].EventName)
    require.Equal(t, int64(10002), dyevt.Records[1].DynamoDB.NewImage.EventMsg.Seq)

    pp := &domain.StockItemCreated{}
    err = dyevt.Records[0].DynamoDB.NewImage.EventMsg.UnmarshalEvent(pp)
    require.NoError(t, err)
    require.Equal(t, evt, pp)
}
