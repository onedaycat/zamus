package sns

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

func TestParseSNSEvent(t *testing.T) {
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
		Time:      1,
	}

	msg1Byte, _ = event.MarshalMsg(msg2)
	data2 := base64.StdEncoding.EncodeToString(msg1Byte)

	payload := fmt.Sprintf(`{
		"Records": [
			{
                "EventVersion": "1.0",
                "EventSubscriptionArn": "arn:aws:sns:us-east-2:123456789012:test-lambda:21be56ed-a058-49f5-8c98-aedd2564c486",
                "EventSource": "aws:sns",
                "Sns": {
                    "SignatureVersion": "1",
                    "Timestamp": "1970-01-01T00:00:00.000Z",
                    "Signature": "tcc6faL2yUC6dgZdmrwh1Y4cGa/ebXEkAi6RibDsvpi+tE/1+82j...65r==",
                    "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-ac565b8b1a6c5d002d285f9598aa1d9b.pem",
                    "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
                    "Message": "Hello from SNS!",
                    "MessageAttributes": {
                        "msg": {
                            "Type": "Binary",
                            "Value": "%s"
                        },
                        "event": {
                            "Type": "String",
                            "Value": "testdata.stock.v1.StockItemCreated"
                        }
                    },
                    "Type": "Notification",
                    "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&amp;SubscriptionArn=arn:aws:sns:us-east-2:123456789012:test-lambda:21be56ed-a058-49f5-8c98-aedd2564c486",
                    "TopicArn": "topicarn",
                    "Subject": "TestInvoke"
                }
            },
            {
                "EventVersion": "1.0",
                "EventSubscriptionArn": "arn:aws:sns:us-east-2:123456789012:test-lambda:21be56ed-a058-49f5-8c98-aedd2564c486",
                "EventSource": "aws:sns",
                "Sns": {
                    "SignatureVersion": "1",
                    "Timestamp": "1970-01-01T00:00:00.000Z",
                    "Signature": "tcc6faL2yUC6dgZdmrwh1Y4cGa/ebXEkAi6RibDsvpi+tE/1+82j...65r==",
                    "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-ac565b8b1a6c5d002d285f9598aa1d9b.pem",
                    "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
                    "Message": "Hello from SNS!",
                    "MessageAttributes": {
                        "msg": {
                            "Type": "Binary",
                            "Value": "%s"
                        },
                        "event": {
                            "Type": "String",
                            "Value": "testdata.stock.v1.StockItemCreated"
                        }
                    },
                    "Type": "Notification",
                    "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&amp;SubscriptionArn=arn:aws:sns:us-east-2:123456789012:test-lambda:21be56ed-a058-49f5-8c98-aedd2564c486",
                    "TopicArn": "topicarn",
                    "Subject": "TestInvoke"
                }
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
