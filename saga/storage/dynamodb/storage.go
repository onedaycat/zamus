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
	hashKeyK = "id"
)

var (
	strongRead = ptr.Bool(true)
)

type DynamoDBSagaStore struct {
	db    *dynamodb.DynamoDB
	table string
	state *saga.State
}

func New(sess *session.Session, sagaTable string) *DynamoDBSagaStore {
	return &DynamoDBSagaStore{
		db:    dynamodb.New(sess),
		table: sagaTable,
		state: &saga.State{},
	}
}

func (s *DynamoDBSagaStore) Truncate() {
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
					"id": &dynamodb.AttributeValue{S: output.Items[i]["id"].S},
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

func (s *DynamoDBSagaStore) CreateSchema(enableStream bool) error {
	_, err := s.db.CreateTable(&dynamodb.CreateTableInput{
		BillingMode: ptr.String("PAY_PER_REQUEST"),
		TableName:   &s.table,
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: ptr.String("id"),
				AttributeType: ptr.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: ptr.String("id"),
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

	return nil
}

func (s *DynamoDBSagaStore) Get(ctx context.Context, id string) (*saga.State, errors.Error) {
	res, err := s.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName:      &s.table,
		ConsistentRead: strongRead,
		Key: map[string]*dynamodb.AttributeValue{
			hashKeyK: &dynamodb.AttributeValue{S: ptr.String(id)},
		},
	})

	if err != nil {
		return nil, appErr.ErrUnableGetState.WithCaller().WithCause(err).WithInput(id)
	}

	if res.Item == nil {
		return nil, appErr.ErrStateNotFound(id).WithCaller().WithInput(id)
	}

	s.state.Clear()
	if err := dynamodbattribute.UnmarshalMap(res.Item, s.state); err != nil {
		return nil, appErr.ErrUnableUnmarshal.WithCaller().WithCause(err).WithInput(res.Item)
	}

	return s.state, nil
}

func (s *DynamoDBSagaStore) Save(ctx context.Context, state *saga.State) errors.Error {

	item, err := dynamodbattribute.MarshalMap(state)
	if err != nil {
		return appErr.ErrUnableMarshal.WithCaller().WithCause(err).WithInput(state)
	}

	_, err = s.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: &s.table,
		Item:      item,
	})

	if err != nil {
		return appErr.ErrUnableSaveState.WithCaller().WithCause(err).WithInput(state)
	}

	return nil
}
