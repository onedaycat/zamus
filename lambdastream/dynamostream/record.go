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

type Records = []*Record

type DynamoDBStreamEvent struct {
	Records Records `json:"Records"`
}

type Record struct {
	EventName string          `json:"eventName"`
	DynamoDB  *DynamoDBRecord `json:"dynamodb"`
}

func (r *Record) add(key, eid, etype string) {
	r.DynamoDB = &DynamoDBRecord{
		NewImage: &Payload{
			EventMsg: &EventMsg{
				PartitionKey: key,
				EventID:      eid,
				EventType:    etype,
			},
		},
	}
}

type DynamoDBRecord struct {
	Keys     map[string]*dynamodb.AttributeValue `json:"Keys"`
	NewImage *Payload                            `json:"NewImage"`
	OldImage *Payload                            `json:"OldImage"`
}
