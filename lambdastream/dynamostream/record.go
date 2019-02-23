package dynamostream

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Payload struct {
	EventMsg *EventMsg
}

func (p *Payload) UnmarshalJSON(b []byte) error {
	var err error
	data := make(map[string]*dynamodb.AttributeValue)
	if err = json.Unmarshal(b, &data); err != nil {
		return err
	}

	return dynamodbattribute.UnmarshalMap(data, &p.EventMsg)
}

const EventInsert = "INSERT"
const eventRemove = "REMOVE"

type Records []*Record

func (a Records) Len() int      { return len(a) }
func (a Records) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a Records) Less(i, j int) bool {
	return a[i].DynamoDB.NewImage.EventMsg.TimeSeq < a[j].DynamoDB.NewImage.EventMsg.TimeSeq
}

type DynamoDBStreamEvent struct {
	Records Records `json:"Records"`
}

func (r *DynamoDBStreamEvent) Add(eventMsg *EventMsg) {
	r.Records = append(r.Records, &Record{
		EventName: EventInsert,
		DynamoDB: &DynamoDBRecord{
			NewImage: &Payload{
				EventMsg: eventMsg,
			},
		},
	})
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
