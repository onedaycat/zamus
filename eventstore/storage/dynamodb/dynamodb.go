package dynamodb

import (
    "context"
    "strconv"
    "time"

    "github.com/aws/aws-sdk-go/aws/awserr"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
    "github.com/onedaycat/errors"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/internal/common/ptr"
)

const (
    hashKeyK = "hk"
    expKeyK  = "exp"
)

type EventStoreStorage struct {
    db              *dynamodb.DynamoDB
    eventstoreTable string
}

func New(db *dynamodb.DynamoDB, eventstoreTable string) *EventStoreStorage {
    return &EventStoreStorage{
        db:              db,
        eventstoreTable: eventstoreTable,
    }
}

func (d *EventStoreStorage) Truncate() {
    output, err := d.db.Scan(&dynamodb.ScanInput{
        TableName: &d.eventstoreTable,
    })
    if err != nil {
        panic(err)
    }
    if len(output.Items) == 0 {
        return
    }

    keyStores := make([]*dynamodb.WriteRequest, len(output.Items))
    for i := 0; i < len(output.Items); i++ {
        keyStores[i] = &dynamodb.WriteRequest{
            DeleteRequest: &dynamodb.DeleteRequest{
                Key: map[string]*dynamodb.AttributeValue{
                    "hk": {S: output.Items[i]["hk"].S},
                },
            },
        }
    }
    _, err = d.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
        RequestItems: map[string][]*dynamodb.WriteRequest{
            d.eventstoreTable: keyStores,
        },
    })
    if err != nil {
        panic(err)
    }
}

func (d *EventStoreStorage) CreateSchema() error {
    _, err := d.db.CreateTable(&dynamodb.CreateTableInput{
        BillingMode: ptr.String("PAY_PER_REQUEST"),
        StreamSpecification: &dynamodb.StreamSpecification{
            StreamEnabled:  ptr.Bool(true),
            StreamViewType: ptr.String("NEW_IMAGE"),
        },
        TableName: &d.eventstoreTable,
        AttributeDefinitions: []*dynamodb.AttributeDefinition{
            {
                AttributeName: ptr.String("hk"),
                AttributeType: ptr.String("S"),
            },
        },
        KeySchema: []*dynamodb.KeySchemaElement{
            {
                AttributeName: ptr.String("hk"),
                KeyType:       ptr.String("HASH"),
            },
        },
    })

    if err != nil {
        aerr, _ := err.(awserr.Error)
        if aerr.Code() != dynamodb.ErrCodeResourceInUseException {
            return err
        }
    }

    for i := 0; i < 10; i++ {
        if _, err := d.db.UpdateTimeToLive(&dynamodb.UpdateTimeToLiveInput{
            TableName: &d.eventstoreTable,
            TimeToLiveSpecification: &dynamodb.TimeToLiveSpecification{
                AttributeName: ptr.String("exp"),
                Enabled:       ptr.Bool(true),
            },
        }); err != nil {
            aerr, _ := err.(awserr.Error)
            if aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
                time.Sleep(time.Second * 5)
                continue
            }
            if aerr.Code() == "ValidationException" && aerr.Message() == "TimeToLive is already enabled" {
                return nil
            }

            return err
        }
    }

    return nil
}

func (d *EventStoreStorage) Save(ctx context.Context, msgs event.Msgs) errors.Error {
    var err error

    var payloadReq map[string]*dynamodb.AttributeValue
    for i := 0; i < len(msgs); i++ {
        payloadReq, err = dynamodbattribute.MarshalMap(msgs[i])
        if err != nil {
            return appErr.ErrUnableSaveEventStore.WithCause(err).WithCaller()
        }

        payloadReq[hashKeyK] = &dynamodb.AttributeValue{S: &msgs[i].Id}
        payloadReq[expKeyK] = &dynamodb.AttributeValue{N: ptr.String(strconv.FormatInt(msgs[i].Time+2592000, 10))}

        _, err = d.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
            TableName: ptr.String(d.eventstoreTable),
            Item:      payloadReq,
        })

        if err != nil {
            return appErr.ErrUnableSaveEventStore.WithCause(err).WithCaller()
        }
    }

    return nil
}
