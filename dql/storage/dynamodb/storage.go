package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common/ptr"
	"github.com/onedaycat/zamus/dql"
	appErr "github.com/onedaycat/zamus/errors"
)

const (
	hashKeyK = "a"
	emptyStr = ""
	seqKV    = ":s"
	getKV    = ":a"
)

var (
	saveCond        = ptr.String("attribute_not_exists(id)")
	saveSnapCond    = ptr.String("attribute_not_exists(s) or s < :s")
	getCond         = ptr.String("a=:a")
	getCondWithTime = ptr.String("a=:a and s > :s")
	falseStrongRead = ptr.Bool(false)
)

type dqlDynamoDB struct {
	db       *dynamodb.DynamoDB
	dqlTable string
}

func New(sess *session.Session, dqlTable string) *dqlDynamoDB {
	return &dqlDynamoDB{
		db:       dynamodb.New(sess),
		dqlTable: dqlTable,
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
					"service": &dynamodb.AttributeValue{S: output.Items[i]["service"].S},
					"id":      &dynamodb.AttributeValue{N: output.Items[i]["id"].N},
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
				AttributeName: ptr.String("service"),
				AttributeType: ptr.String("S"),
			},
			{
				AttributeName: ptr.String("id"),
				AttributeType: ptr.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: ptr.String("service"),
				KeyType:       ptr.String("HASH"),
			},
			{
				AttributeName: ptr.String("id"),
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

	_, err := d.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: &d.dqlTable,
		Item:      item,
	})

	if err != nil {
		return appErr.ErrUnbleSaveDQLMessages.WithCaller().WithCause(err)
	}

	return nil
}
