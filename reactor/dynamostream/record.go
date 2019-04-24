package dynamostream

import (
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
    "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common"
)

type Payload struct {
    EventMsg *event.Msg
}

func (p *Payload) UnmarshalJSON(b []byte) error {
    var err error
    data := make(map[string]*dynamodb.AttributeValue)
    if err = common.UnmarshalJSON(b, &data); err != nil {
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

type EventSource struct {
    Records    Records `json:"Records"`
    Warmer     bool    `json:"warmer,omitempty"`
    Concurency int     `json:"concurency,omitempty"`
}

func (e *EventSource) Clear() {
    e.Records = e.Records[:0]
    e.Warmer = false
    e.Concurency = 0
}

type Record struct {
    EventName string          `json:"eventName"`
    DynamoDB  *DynamoDBRecord `json:"dynamodb"`
}

func (r *Record) add(key, eid, etype string) {
    r.DynamoDB = &DynamoDBRecord{
        NewImage: &Payload{
            EventMsg: &event.Msg{
                Id:        eid,
                AggID:     key,
                EventType: etype,
            },
        },
    }
}

type DynamoDBRecord struct {
    NewImage *Payload `json:"NewImage"`
}
