package dynamodb

import (
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/onedaycat/zamus/eventstore"
)

const (
	hashKeyK = "a"
	emptyStr = ""
	seqKV    = ":x"
	getKV    = ":a"
)

var (
	saveCond        = aws.String("attribute_not_exists(x)")
	saveSnapCond    = aws.String("attribute_not_exists(x) or x < :x")
	getCond         = aws.String("a=:a")
	getCondWithTime = aws.String("a=:a and x > :x")
	falseStrongRead = aws.Bool(false)
)

type DynamoDBEventStore struct {
	db              *dynamodb.DynamoDB
	eventstoreTable string
	snapshotTable   string
}

func New(sess *session.Session, eventstoreTable, snapshotTable string) *DynamoDBEventStore {
	return &DynamoDBEventStore{
		db:              dynamodb.New(sess),
		eventstoreTable: eventstoreTable,
		snapshotTable:   snapshotTable,
	}
}

func (d *DynamoDBEventStore) Truncate() {
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
					"a": &dynamodb.AttributeValue{S: output.Items[i]["a"].S},
					"x": &dynamodb.AttributeValue{N: output.Items[i]["x"].N},
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

	output, err = d.db.Scan(&dynamodb.ScanInput{
		TableName: &d.snapshotTable,
	})
	if err != nil {
		panic(err)
	}
	if len(output.Items) == 0 {
		return
	}
	keyStores = make([]*dynamodb.WriteRequest, len(output.Items))
	for i := 0; i < len(output.Items); i++ {
		keyStores[i] = &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: map[string]*dynamodb.AttributeValue{
					"a": &dynamodb.AttributeValue{S: output.Items[i]["a"].S},
				},
			},
		}
	}
	_, err = d.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			d.snapshotTable: keyStores,
		},
	})
	if err != nil {
		panic(err)
	}
}

func (d *DynamoDBEventStore) CreateSchema(enableStream bool) error {
	_, err := d.db.CreateTable(&dynamodb.CreateTableInput{
		BillingMode: aws.String("PAY_PER_REQUEST"),
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(enableStream),
			StreamViewType: aws.String("NEW_IMAGE"),
		},
		TableName: &d.eventstoreTable,
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("a"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("x"),
				AttributeType: aws.String("N"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("a"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("x"),
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

	_, err = d.db.CreateTable(&dynamodb.CreateTableInput{
		BillingMode: aws.String("PAY_PER_REQUEST"),
		TableName:   &d.snapshotTable,
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("a"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("a"),
				KeyType:       aws.String("HASH"),
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

func (d *DynamoDBEventStore) GetEvents(aggID string, seq, limit int64) ([]*eventstore.EventMsg, error) {
	keyCond := getCond
	exValue := map[string]*dynamodb.AttributeValue{
		getKV: &dynamodb.AttributeValue{S: &aggID},
	}

	if seq > 0 {
		exValue[seqKV] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(seq, 10))}
		keyCond = getCondWithTime
	}

	output, err := d.db.Query(&dynamodb.QueryInput{
		TableName:              &d.eventstoreTable,
		KeyConditionExpression: keyCond,
		// Limit:                     &limit,
		ExpressionAttributeValues: exValue,
		ConsistentRead:            falseStrongRead,
	})

	if err != nil {
		return nil, err
	}

	if len(output.Items) == 0 {
		return nil, nil
	}

	msgs := make([]*eventstore.EventMsg, 0, len(output.Items))
	if err = dynamodbattribute.UnmarshalListOfMaps(output.Items, &msgs); err != nil {
		return nil, err
	}

	return msgs, nil
}

func (d *DynamoDBEventStore) GetSnapshot(aggID string) (*eventstore.Snapshot, error) {
	output, err := d.db.GetItem(&dynamodb.GetItemInput{
		TableName:      &d.snapshotTable,
		ConsistentRead: falseStrongRead,
		Key: map[string]*dynamodb.AttributeValue{
			hashKeyK: &dynamodb.AttributeValue{S: &aggID},
		},
	})
	if err != nil {
		return nil, err
	}

	if len(output.Item) == 0 {
		return nil, eventstore.ErrNotFound
	}

	snapshot := &eventstore.Snapshot{}
	if err = dynamodbattribute.UnmarshalMap(output.Item, snapshot); err != nil {
		return nil, err
	}

	return snapshot, nil
}

func (d *DynamoDBEventStore) SaveV1(msgs []*eventstore.EventMsg, snapshot *eventstore.Snapshot) error {
	var err error

	var putES []*dynamodb.TransactWriteItem
	var payloadReq map[string]*dynamodb.AttributeValue

	if snapshot != nil {
		var snapshotReq map[string]*dynamodb.AttributeValue
		snapshotReq, err = dynamodbattribute.MarshalMap(snapshot)
		if err != nil {
			return err
		}

		putES = make([]*dynamodb.TransactWriteItem, 0, len(msgs)+1)
		putES = append(putES, &dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				TableName: &d.snapshotTable,
				// ConditionExpression: saveSnapCond,
				// ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				// 	seqKV: &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(snapshot.TimeSeq, 10))},
				// },
				Item: snapshotReq,
			},
		})
	} else {
		putES = make([]*dynamodb.TransactWriteItem, 0, len(msgs))
	}

	for i := 0; i < len(msgs); i++ {
		payloadReq, err = dynamodbattribute.MarshalMap(msgs[i])
		if err != nil {
			return err
		}

		putES = append(putES, &dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				TableName:           &d.eventstoreTable,
				ConditionExpression: saveCond,
				Item:                payloadReq,
			},
		})
	}

	_, err = d.db.TransactWriteItems(&dynamodb.TransactWriteItemsInput{
		TransactItems: putES,
	})

	if err != nil {
		aerr := err.(awserr.Error)
		if aerr.Code() == dynamodb.ErrCodeTransactionCanceledException {
			return eventstore.ErrVersionInconsistency
		}

		return err
	}

	return nil
}

func (d *DynamoDBEventStore) Save(msgs []*eventstore.EventMsg, snapshot *eventstore.Snapshot) error {
	var err error

	var payloadReq map[string]*dynamodb.AttributeValue
	for i := 0; i < len(msgs); i++ {
		payloadReq, err = dynamodbattribute.MarshalMap(msgs[i])
		if err != nil {
			return err
		}

		_, err = d.db.PutItem(&dynamodb.PutItemInput{
			TableName:           &d.eventstoreTable,
			ConditionExpression: saveCond,
			Item:                payloadReq,
		})

		if err != nil {
			aerr := err.(awserr.Error)
			if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return eventstore.ErrVersionInconsistency
			}

			return err
		}
	}

	if snapshot == nil {
		return nil
	}

	var snapshotReq map[string]*dynamodb.AttributeValue
	snapshotReq, err = dynamodbattribute.MarshalMap(snapshot)
	if err != nil {
		return err
	}

	_, err = d.db.PutItem(&dynamodb.PutItemInput{
		TableName: &d.snapshotTable,
		Item:      snapshotReq,
	})

	if err != nil {
		aerr := err.(awserr.Error)
		if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return eventstore.ErrVersionInconsistency
		}

		return err
	}

	return err
}
