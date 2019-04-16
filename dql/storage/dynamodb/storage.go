package dynamodb

import (
    "context"

    "github.com/aws/aws-sdk-go/aws/awserr"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/dql"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/internal/common/ptr"
)

type dqlDynamoDB struct {
    db         *dynamodb.DynamoDB
    dqlTable   string
    hkVal      string
    delim      string
    strongRead *bool
    hashKeyK   string
    rangeKeyK  string
}

func New(sess *session.Session, dqlTable string) *dqlDynamoDB {
    return &dqlDynamoDB{
        db:         dynamodb.New(sess),
        dqlTable:   dqlTable,
        hkVal:      "zamus",
        delim:      "_",
        strongRead: ptr.Bool(false),
        hashKeyK:   "hk",
        rangeKeyK:  "rk",
    }
}

func (d *dqlDynamoDB) Truncate() {
    output, err := d.db.Scan(&dynamodb.ScanInput{
        TableName: &d.dqlTable,
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
            d.dqlTable: keyStores,
        },
    })
    if err != nil {
        panic(err)
    }
}

func (d *dqlDynamoDB) CreateSchema(enableStream bool) error {
    _, err := d.db.CreateTable(&dynamodb.CreateTableInput{
        BillingMode: ptr.String("PAY_PER_REQUEST"),
        StreamSpecification: &dynamodb.StreamSpecification{
            StreamEnabled:  ptr.Bool(enableStream),
            StreamViewType: ptr.String("NEW_IMAGE"),
        },
        TableName: &d.dqlTable,
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

func (d *dqlDynamoDB) Save(ctx context.Context, dqlMsg *dql.DQLMsg) errors.Error {
    item, _ := dynamodbattribute.MarshalMap(dqlMsg)

    item["hk"] = &dynamodb.AttributeValue{S: ptr.String(d.hkVal)}
    item["rk"] = &dynamodb.AttributeValue{S: ptr.String(dqlMsg.Service + d.delim + dqlMsg.ID)}
    _, err := d.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
        TableName: &d.dqlTable,
        Item:      item,
    })

    if err != nil {
        return appErr.ErrUnableSaveDQLMessages.WithCaller().WithCause(err)
    }

    return nil
}

func (d *dqlDynamoDB) Get(ctx context.Context, service, id string) (*dql.DQLMsg, errors.Error) {
    res, err := d.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
        TableName:      &d.dqlTable,
        ConsistentRead: d.strongRead,
        Key: map[string]*dynamodb.AttributeValue{
            d.hashKeyK:  {S: ptr.String(d.hkVal)},
            d.rangeKeyK: {S: ptr.String(service + d.delim + id)},
        },
    })

    if err != nil {
        return nil, appErr.ErrUnableGetDQLMsg.WithCaller().WithCause(err).WithInput(id)
    }

    if res.Item == nil {
        return nil, appErr.ErrDQLMsgNotFound.WithCaller().WithInput(id)
    }

    msg := &dql.DQLMsg{}
    if err := dynamodbattribute.UnmarshalMap(res.Item, msg); err != nil {
        return nil, appErr.ErrUnableUnmarshal.WithCaller().WithCause(err).WithInput(res.Item)
    }

    return msg, nil
}
