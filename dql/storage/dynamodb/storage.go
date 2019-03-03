package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/onedaycat/zamus/dql"
	"github.com/onedaycat/zamus/errors"
)

const (
	hashKeyK = "a"
	emptyStr = ""
	seqKV    = ":s"
	getKV    = ":a"
)

var (
	saveCond        = aws.String("attribute_not_exists(id)")
	saveSnapCond    = aws.String("attribute_not_exists(s) or s < :s")
	getCond         = aws.String("a=:a")
	getCondWithTime = aws.String("a=:a and s > :s")
	falseStrongRead = aws.Bool(false)
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
		BillingMode: aws.String("PAY_PER_REQUEST"),
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(enableStream),
			StreamViewType: aws.String("NEW_IMAGE"),
		},
		TableName: &d.dqlTable,
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("service"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("service"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String("RANGE"),
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

func (d *dqlDynamoDB) MultiSave(ctx context.Context, msgs dql.DQLMsgs) error {
	var item map[string]*dynamodb.AttributeValue
	items := make([]*dynamodb.WriteRequest, 0, len(msgs))
	for _, msg := range msgs {
		item, _ = dynamodbattribute.MarshalMap(msg)
		items = append(items, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		})
	}

	_, err := d.db.BatchWriteItemWithContext(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			d.dqlTable: items,
		},
	})

	if err != nil {
		return errors.ErrUnbleSaveDQLMessages.WithCaller().WithCause(err)
	}

	return nil
}
