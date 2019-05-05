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

type Source struct {
    Msgs       []byte  `json:"msgs,omitempty"`
    Records    Records `json:"Records"`
    Warmer     bool    `json:"warmer,omitempty"`
    Concurency int     `json:"concurency,omitempty"`
}

func (e *Source) Clear() {
    e.Records = e.Records[:0]
    e.Warmer = false
    e.Concurency = 0
}

type Record struct {
    EventName string          `json:"eventName"`
    DynamoDB  *DynamoDBRecord `json:"dynamodb"`
}

type DynamoDBRecord struct {
    NewImage *Payload `json:"NewImage"`
}
