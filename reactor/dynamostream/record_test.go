package dynamostream

import (
	"encoding/base64"
	"fmt"
	"sort"
	"testing"

	"github.com/golang/snappy"
	"github.com/onedaycat/zamus/common"

	"github.com/stretchr/testify/require"
)

func TestParseDynamoDBStreamEvent(t *testing.T) {
	p, err := common.MarshalJSON(map[string]interface{}{
		"id": "1",
	})
	require.NoError(t, err)

	var dst []byte
	dst = snappy.Encode(dst, p)
	p64 := base64.StdEncoding.EncodeToString(dst)

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
						"a": {
							"S": "a1"
						},
						"b": {
							"S": "domain.aggregate"
						},
						"v": {
							"N": "10"
						},
						"b": {
							"S": "domain.aggregate.event"
						},
						"s": {
							"N": "10002"
						},
						"e": {
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
					"a": {
						"S": "a1"
					},
					"b": {
						"S": "domain.aggregate"
					},
					"v": {
						"N": "10"
					},
					"b": {
						"S": "domain.aggregate.event"
					},
					"s": {
						"N": "10001"
					},
					"e": {
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

	type pdata struct {
		ID string `json:"id"`
	}

	event := &DynamoDBStreamEvent{}
	err = common.UnmarshalJSON([]byte(payload), event)
	sort.Sort(event.Records)
	require.NoError(t, err)
	require.Len(t, event.Records, 2)
	require.Equal(t, eventRemove, event.Records[0].EventName)
	require.Equal(t, int64(10001), event.Records[0].DynamoDB.NewImage.EventMsg.Seq)
	require.Equal(t, EventInsert, event.Records[1].EventName)
	require.Equal(t, int64(10002), event.Records[1].DynamoDB.NewImage.EventMsg.Seq)

	xx, _ := common.MarshalJSON(event.Records[0].DynamoDB.NewImage.EventMsg)
	fmt.Println(string(xx))

	pp := &pdata{}
	event.Records[0].DynamoDB.NewImage.EventMsg.UnmarshalEvent(pp)
	require.NoError(t, err)
	fmt.Println(pp)
	require.Equal(t, &pdata{"1"}, pp)
}