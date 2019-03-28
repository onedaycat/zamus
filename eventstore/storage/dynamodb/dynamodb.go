package dynamodb

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common/ptr"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/eventstore"
)

const (
	hashKeyK      = "a"
	snapRangeKeyK = "v"
	emptyStr      = ""
	seqKV         = ":s"
	verKV         = ":v"
	getKV         = ":a"
)

var (
	saveCond            = ptr.String("attribute_not_exists(s)")
	saveSnapCond        = ptr.String("attribute_not_exists(s) or s < :s")
	getCond             = ptr.String("a=:a")
	snapshotCond        = ptr.String("a=:a")
	snapshotCondWithVer = ptr.String("a=:a and v = :v")
	getCondWithTime     = ptr.String("a=:a and s > :s")
	falseStrongRead     = ptr.Bool(false)
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
					"s": &dynamodb.AttributeValue{N: output.Items[i]["s"].N},
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
					"v": &dynamodb.AttributeValue{N: output.Items[i]["v"].N},
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
		BillingMode: ptr.String("PAY_PER_REQUEST"),
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled:  ptr.Bool(enableStream),
			StreamViewType: ptr.String("NEW_IMAGE"),
		},
		TableName: &d.eventstoreTable,
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: ptr.String("a"),
				AttributeType: ptr.String("S"),
			},
			{
				AttributeName: ptr.String("s"),
				AttributeType: ptr.String("N"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: ptr.String("a"),
				KeyType:       ptr.String("HASH"),
			},
			{
				AttributeName: ptr.String("s"),
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

	_, err = d.db.CreateTable(&dynamodb.CreateTableInput{
		BillingMode: ptr.String("PAY_PER_REQUEST"),
		TableName:   &d.snapshotTable,
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: ptr.String("a"),
				AttributeType: ptr.String("S"),
			},
			{
				AttributeName: ptr.String("v"),
				AttributeType: ptr.String("N"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: ptr.String("a"),
				KeyType:       ptr.String("HASH"),
			},
			{
				AttributeName: ptr.String("v"),
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

func (d *DynamoDBEventStore) GetEvents(ctx context.Context, aggID string, seq int64) ([]*eventstore.EventMsg, errors.Error) {
	keyCond := getCond
	exValue := map[string]*dynamodb.AttributeValue{
		getKV: &dynamodb.AttributeValue{S: &aggID},
	}

	if seq > 0 {
		exValue[seqKV] = &dynamodb.AttributeValue{N: ptr.String(strconv.FormatInt(seq, 10))}
		keyCond = getCondWithTime
	}

	msgs := make([]*eventstore.EventMsg, 0, 100)
	err := d.db.QueryPagesWithContext(ctx, &dynamodb.QueryInput{
		TableName:                 &d.eventstoreTable,
		KeyConditionExpression:    keyCond,
		ExpressionAttributeValues: exValue,
		ConsistentRead:            falseStrongRead,
	}, func(output *dynamodb.QueryOutput, lastPage bool) bool {
		if len(output.Items) == 0 {
			return false
		}

		submsgs := make([]*eventstore.EventMsg, 0, len(output.Items))
		dynamodbattribute.UnmarshalListOfMaps(output.Items, &submsgs)
		msgs = append(msgs, submsgs...)

		return !lastPage
	})

	if err != nil {
		return nil, appErr.ErrUnbleGetEventStore.WithCause(err).WithCaller().WithInput(errors.Input{
			"aggID": aggID,
			"seq":   seq,
		})
	}

	if len(msgs) == 0 {
		return nil, nil
	}

	return msgs, nil
}

func (d *DynamoDBEventStore) GetSnapshot(ctx context.Context, aggID string, version int) (*eventstore.Snapshot, errors.Error) {
	if version == 0 {
		return nil, nil
	}

	output, err := d.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName:      &d.snapshotTable,
		ConsistentRead: falseStrongRead,
		Key: map[string]*dynamodb.AttributeValue{
			hashKeyK:      &dynamodb.AttributeValue{S: &aggID},
			snapRangeKeyK: &dynamodb.AttributeValue{N: ptr.String(strconv.Itoa(version))},
		},
	})
	if err != nil {
		return nil, appErr.ErrUnbleGetEventStore.WithCause(err).WithCaller().WithInput(aggID)
	}

	if len(output.Item) == 0 {
		return nil, nil
	}

	snapshot := &eventstore.Snapshot{}
	if err = dynamodbattribute.UnmarshalMap(output.Item, snapshot); err != nil {
		return nil, appErr.ErrUnbleGetEventStore.WithCause(err).WithCaller().WithInput(errors.Input{
			"aggID":   aggID,
			"version": version,
		})
	}

	return snapshot, nil
}

// func (d *DynamoDBEventStore) SaveV1(ctx context.Context, msgs []*eventstore.EventMsg, snapshot *eventstore.Snapshot) errors.Error {
// 	var err error

// 	var putES []*dynamodb.TransactWriteItem
// 	var payloadReq map[string]*dynamodb.AttributeValue

// 	if snapshot != nil {
// 		var snapshotReq map[string]*dynamodb.AttributeValue
// 		snapshotReq, err = dynamodbattribute.MarshalMap(snapshot)
// 		if err != nil {
// 			return err
// 		}

// 		putES = make([]*dynamodb.TransactWriteItem, 0, len(msgs)+1)
// 		putES = append(putES, &dynamodb.TransactWriteItem{
// 			Put: &dynamodb.Put{
// 				TableName: &d.snapshotTable,
// 				// ConditionExpression: saveSnapCond,
// 				// ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
// 				// 	seqKV: &dynamodb.AttributeValue{N: ptr.String(strconv.FormatInt(snapshot.TimeSeq, 10))},
// 				// },
// 				Item: snapshotReq,
// 			},
// 		})
// 	} else {
// 		putES = make([]*dynamodb.TransactWriteItem, 0, len(msgs))
// 	}

// 	for i := 0; i < len(msgs); i++ {
// 		payloadReq, err = dynamodbattribute.MarshalMap(msgs[i])
// 		if err != nil {
// 			return err
// 		}

// 		putES = append(putES, &dynamodb.TransactWriteItem{
// 			Put: &dynamodb.Put{
// 				TableName:           &d.eventstoreTable,
// 				ConditionExpression: saveCond,
// 				Item:                payloadReq,
// 			},
// 		})
// 	}

// 	_, err = d.db.TransactWriteItemsWithContext(ctx, &dynamodb.TransactWriteItemsInput{
// 		TransactItems: putES,
// 	})

// 	if err != nil {
// 		aerr := err.(awserr.Error)
// 		if aerr.Code() == dynamodb.ErrCodeTransactionCanceledException {
// 			return errors.ErrVersionInconsistency.WithCaller()
// 		}

// 		return errors.Warp(err).WithCaller()
// 	}

// 	return nil
// }

func (d *DynamoDBEventStore) Save(ctx context.Context, msgs []*eventstore.EventMsg, snapshot *eventstore.Snapshot) errors.Error {
	if err := d.saveEvents(ctx, msgs); err != nil {
		return err
	}

	return d.saveSnapshot(ctx, snapshot)
}

func (d *DynamoDBEventStore) saveEvents(ctx context.Context, msgs []*eventstore.EventMsg) errors.Error {
	var err error

	var payloadReq map[string]*dynamodb.AttributeValue
	for i := 0; i < len(msgs); i++ {
		payloadReq, err = dynamodbattribute.MarshalMap(msgs[i])
		if err != nil {
			return appErr.ErrUnbleSaveEventStore.WithCause(err).WithCaller()
		}

		_, err = d.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
			TableName:           &d.eventstoreTable,
			ConditionExpression: saveCond,
			Item:                payloadReq,
		})

		if err != nil {
			aerr := err.(awserr.Error)
			if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return appErr.ErrVersionInconsistency.WithCaller()
			}

			return appErr.ErrUnbleSaveEventStore.WithCause(err).WithCaller()
		}
	}

	return nil
}

func (d *DynamoDBEventStore) saveSnapshot(ctx context.Context, snapshot *eventstore.Snapshot) errors.Error {
	if snapshot == nil {
		return nil
	}

	var snapshotReq map[string]*dynamodb.AttributeValue
	var xerr error
	snapshotReq, xerr = dynamodbattribute.MarshalMap(snapshot)
	if xerr != nil {
		return appErr.ErrUnbleSaveEventStore.WithCause(xerr).WithCaller()
	}

	_, xerr = d.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: &d.snapshotTable,
		Item:      snapshotReq,
	})

	if xerr != nil {
		aerr := xerr.(awserr.Error)
		if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return appErr.ErrVersionInconsistency.WithCaller()
		}

		return appErr.ErrUnbleSaveEventStore.WithCause(xerr).WithCaller()
	}

	return nil
}
