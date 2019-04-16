package dynamodb

import (
    "context"

    "github.com/aws/aws-sdk-go/aws/awserr"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/common/ptr"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/saga"
)

const (
    hashKeyK  = "hk"
    rangeKeyK = "rk"
    hkVal     = "zamus"
    delim     = "_"
)

var (
    strongRead = ptr.Bool(true)
)

type SagaStore struct {
    db    *dynamodb.DynamoDB
    table string
    state *saga.State
}

func New(sess *session.Session, sagaTable string) *SagaStore {
    return &SagaStore{
        db:    dynamodb.New(sess),
        table: sagaTable,
        state: &saga.State{},
    }
}

func (s *SagaStore) Truncate() {
    output, err := s.db.Scan(&dynamodb.ScanInput{
        TableName: &s.table,
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
    _, err = s.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
        RequestItems: map[string][]*dynamodb.WriteRequest{
            s.table: keyStores,
        },
    })
    if err != nil {
        panic(err)
    }
}

func (s *SagaStore) CreateSchema(enableStream bool) error {
    _, err := s.db.CreateTable(&dynamodb.CreateTableInput{
        BillingMode: ptr.String("PAY_PER_REQUEST"),
        TableName:   &s.table,
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

func (s *SagaStore) Get(ctx context.Context, stateName, id string) (*saga.State, errors.Error) {
    res, err := s.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
        TableName:      &s.table,
        ConsistentRead: strongRead,
        Key: map[string]*dynamodb.AttributeValue{
            hashKeyK:  {S: ptr.String(hkVal)},
            rangeKeyK: {S: ptr.String(stateName + delim + id)},
        },
    })

    if err != nil {
        return nil, appErr.ErrUnableGetState.WithCaller().WithCause(err).WithInput(id)
    }

    if res.Item == nil {
        return nil, appErr.ErrStateNotFound.WithCaller().WithInput(id)
    }

    s.state.Clear()
    if err := dynamodbattribute.UnmarshalMap(res.Item, s.state); err != nil {
        return nil, appErr.ErrUnableUnmarshal.WithCaller().WithCause(err).WithInput(res.Item)
    }

    return s.state, nil
}

func (s *SagaStore) Save(ctx context.Context, state *saga.State) errors.Error {

    item, err := dynamodbattribute.MarshalMap(state)
    if err != nil {
        return appErr.ErrUnableMarshal.WithCaller().WithCause(err).WithInput(state)
    }

    item["hk"] = &dynamodb.AttributeValue{S: ptr.String(hkVal)}
    item["rk"] = &dynamodb.AttributeValue{S: ptr.String(state.Name + delim + state.ID)}
    _, err = s.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
        TableName: &s.table,
        Item:      item,
    })

    if err != nil {
        return appErr.ErrUnableSaveState.WithCaller().WithCause(err).WithInput(state)
    }

    return nil
}
