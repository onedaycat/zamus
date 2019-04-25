package dynamodb

import (
    "context"

    "github.com/aws/aws-sdk-go/aws/awserr"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/dlq"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/internal/common/ptr"
)

type dlqDynamoDB struct {
    db         *dynamodb.DynamoDB
    dlqTable   string
    hkVal      string
    delim      string
    strongRead *bool
    hashKeyK   string
    rangeKeyK  string
}

func New(sess *session.Session, dlqTable string) *dlqDynamoDB {
    return &dlqDynamoDB{
        db:         dynamodb.New(sess),
        dlqTable:   dlqTable,
        hkVal:      "zamus",
        delim:      "_",
        strongRead: ptr.Bool(false),
        hashKeyK:   "hk",
        rangeKeyK:  "rk",
    }
}

func (d *dlqDynamoDB) Truncate() {
    output, err := d.db.Scan(&dynamodb.ScanInput{
        TableName: &d.dlqTable,
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
                    "rk": {S: output.Items[i]["rk"].S},
                },
            },
        }
    }
    _, err = d.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
        RequestItems: map[string][]*dynamodb.WriteRequest{
            d.dlqTable: keyStores,
        },
    })
    if err != nil {
        panic(err)
    }
}

func (d *dlqDynamoDB) CreateSchema(enableStream bool) error {
    _, err := d.db.CreateTable(&dynamodb.CreateTableInput{
        BillingMode: ptr.String("PAY_PER_REQUEST"),
        StreamSpecification: &dynamodb.StreamSpecification{
            StreamEnabled:  ptr.Bool(enableStream),
            StreamViewType: ptr.String("NEW_IMAGE"),
        },
        TableName: &d.dlqTable,
        AttributeDefinitions: []*dynamodb.AttributeDefinition{
            {
                AttributeName: ptr.String("hk"),
                AttributeType: ptr.String("S"),
            },
            {
                AttributeName: ptr.String("rk"),
                AttributeType: ptr.String("S"),
            },
        },
        KeySchema: []*dynamodb.KeySchemaElement{
            {
                AttributeName: ptr.String("hk"),
                KeyType:       ptr.String("HASH"),
            },
            {
                AttributeName: ptr.String("rk"),
                KeyType:       ptr.String("RANGE"),
            },
        },
    })

    if err != nil {
        aerr, _ := err.(awserr.Error)
        if aerr.Code() != dynamodb.ErrCodeResourceInUseException {
            return err
        }
    }

    return nil
}

func (d *dlqDynamoDB) Save(ctx context.Context, dlqMsg *dlq.DLQMsg) errors.Error {
    item, _ := dynamodbattribute.MarshalMap(dlqMsg)

    item["hk"] = &dynamodb.AttributeValue{S: ptr.String(d.hkVal)}
    item["rk"] = &dynamodb.AttributeValue{S: ptr.String(dlqMsg.Service + d.delim + dlqMsg.ID)}
    _, err := d.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
        TableName: &d.dlqTable,
        Item:      item,
    })

    if err != nil {
        return appErr.ErrUnableSaveDLQMessages.WithCaller().WithCause(err)
    }

    return nil
}

func (d *dlqDynamoDB) Get(ctx context.Context, service, id string) (*dlq.DLQMsg, errors.Error) {
    res, err := d.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
        TableName:      &d.dlqTable,
        ConsistentRead: d.strongRead,
        Key: map[string]*dynamodb.AttributeValue{
            d.hashKeyK:  {S: ptr.String(d.hkVal)},
            d.rangeKeyK: {S: ptr.String(service + d.delim + id)},
        },
    })

    if err != nil {
        return nil, appErr.ErrUnableGetDLQMsg.WithCaller().WithCause(err).WithInput(id)
    }

    if res.Item == nil {
        return nil, appErr.ErrDLQMsgNotFound.WithCaller().WithInput(id)
    }

    msg := &dlq.DLQMsg{}
    if err := dynamodbattribute.UnmarshalMap(res.Item, msg); err != nil {
        return nil, appErr.ErrUnableUnmarshal.WithCaller().WithCause(err).WithInput(res.Item)
    }

    return msg, nil
}
