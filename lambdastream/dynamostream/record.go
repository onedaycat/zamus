package dynamostream

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Payload struct {
	EventMessage *EventMessage
}

func (p *Payload) UnmarshalJSON(b []byte) error {
	var err error
	data := make(map[string]*dynamodb.AttributeValue)
	if err = json.Unmarshal(b, &data); err != nil {
		return err
	}

	event := &EventMessage{}
	if err = dynamodbattribute.UnmarshalMap(data, event); err != nil {
		return err
	}

	p.EventMessage = event
	p.EventMessage.Payload = data["p"].B

	return nil
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

func (r *Record) add(key, val, eid string) {
	r.DynamoDB = &DynamoDBRecord{
		Keys: map[string]*dynamodb.AttributeValue{
			key: &dynamodb.AttributeValue{S: &val},
		},
		NewImage: &Payload{
			EventMessage: &EventMessage{
				EventID: eid,
			},
		},
	}
}

type DynamoDBRecord struct {
	Keys     map[string]*dynamodb.AttributeValue `json:"Keys"`
	NewImage *Payload                            `json:"NewImage"`
	OldImage *Payload                            `json:"OldImage"`
}
