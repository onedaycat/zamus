package dynamostream

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/onedaycat/zamus/errors"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Payload struct {
	EventMsg *EventMsg
}

func (p *Payload) UnmarshalJSON(b []byte) error {
	var err error
	data := make(map[string]*dynamodb.AttributeValue)
	if err = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(b, &data); err != nil {
		return errors.ErrUnableUnmarshal.WithCaller().WithCause(err)
	}

	if err = dynamodbattribute.UnmarshalMap(data, &p.EventMsg); err != nil {
		return errors.ErrUnableUnmarshal.WithCaller().WithCause(err)
	}

	return nil
}

const EventInsert = "INSERT"
const eventRemove = "REMOVE"

type Records []*Record

func (a Records) Len() int      { return len(a) }
func (a Records) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a Records) Less(i, j int) bool {
	return a[i].DynamoDB.NewImage.EventMsg.Seq < a[j].DynamoDB.NewImage.EventMsg.Seq
}

type DynamoDBStreamEvent struct {
	Records    Records `json:"Records"`
	Warmer     bool    `json:"warmer,omitempty"`
	Concurency int     `json:"concurency,omitempty"`
}

type Record struct {
	EventName string          `json:"eventName"`
	DynamoDB  *DynamoDBRecord `json:"dynamodb"`
}

func (r *Record) add(key, eid, etype string) {
	r.DynamoDB = &DynamoDBRecord{
		NewImage: &Payload{
			EventMsg: &EventMsg{
				AggregateID: key,
				EventID:     eid,
				EventType:   etype,
			},
		},
	}
}

type DynamoDBRecord struct {
	NewImage *Payload `json:"NewImage"`
}
