package dynamodb

import (
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/onedaycat/zamus/eventstore"
)

const (
	hashKeyK             = "a"
	emptyStr             = ""
	seqKV                = ":x"
	getKV                = ":a"
	getByEventTypeKV     = ":et"
	getByAggregateTypeKV = ":b"
)

var (
	eSIndex                        = aws.String("e-x-index")
	bSIndex                        = aws.String("b-x-index")
	saveCond                       = aws.String("attribute_not_exists(x)")
	saveSnapCond                   = aws.String("attribute_not_exists(x) or x < :x")
	getCond                        = aws.String("a=:a")
	getCondWithTime                = aws.String("a=:a and x > :x")
	getByEventTypeCond             = aws.String("e=:et")
	getByEventTypeWithTimeCond     = aws.String("e=:et and x > :x")
	getByAggregateTypeCond         = aws.String("b=:b")
	getByAggregateTypeWithTimeCond = aws.String("b=:b and x > :x")
	falseStrongRead                = aws.Bool(false)
)

type DynamoDBEventStore struct {
	db              *dynamodb.DynamoDB
	eventstoreTable string
	snapshotTable   string
	aggregateTable  string
	ttl             time.Duration
}

func New(sess *session.Session, eventstoreTable, aggregateTable, snapshotTable string) *DynamoDBEventStore {
	return &DynamoDBEventStore{
		db:              dynamodb.New(sess),
		eventstoreTable: eventstoreTable,
		snapshotTable:   snapshotTable,
		aggregateTable:  aggregateTable,
		ttl:             24 * 30 * time.Hour,
	}
}

func (d *DynamoDBEventStore) SetTTL(ttl time.Duration) {
	d.ttl = ttl
}

func (d *DynamoDBEventStore) TruncateTables() {
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
					"h": &dynamodb.AttributeValue{S: output.Items[i]["h"].S},
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
					"h": &dynamodb.AttributeValue{S: output.Items[i]["h"].S},
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
				AttributeName: aws.String("e"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("b"),
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
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{
			{
				IndexName: aws.String("e-x-index"),
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String("ALL"),
				},
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("e"),
						KeyType:       aws.String("HASH"),
					},
					{
						AttributeName: aws.String("x"),
						KeyType:       aws.String("RANGE"),
					},
				},
			},
			{
				IndexName: aws.String("b-x-index"),
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String("ALL"),
				},
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("b"),
						KeyType:       aws.String("HASH"),
					},
					{
						AttributeName: aws.String("x"),
						KeyType:       aws.String("RANGE"),
					},
				},
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
		TableName:   &d.aggregateTable,
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

	_, err = d.db.CreateTable(&dynamodb.CreateTableInput{
		BillingMode: aws.String("PAY_PER_REQUEST"),
		TableName:   &d.snapshotTable,
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
		TableName:                 &d.eventstoreTable,
		KeyConditionExpression:    keyCond,
		Limit:                     &limit,
		ExpressionAttributeValues: exValue,
		ConsistentRead:            falseStrongRead,
	})

	if err != nil {
		return nil, err
	}

	if len(output.Items) == 0 {
		return nil, nil
	}

	snapshots := make([]*eventstore.EventMsg, 0, len(output.Items))
	if err = dynamodbattribute.UnmarshalListOfMaps(output.Items, &snapshots); err != nil {
		return nil, err
	}

	return snapshots, nil
}

func (d *DynamoDBEventStore) GetEventsByEventType(eventType string, seq, limit int64) ([]*eventstore.EventMsg, error) {
	keyCond := getByEventTypeCond
	exValue := map[string]*dynamodb.AttributeValue{
		getByEventTypeKV: &dynamodb.AttributeValue{S: &eventType},
	}

	if seq > 0 {
		exValue[seqKV] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(seq, 10))}
		keyCond = getByEventTypeWithTimeCond
	}

	output, err := d.db.Query(&dynamodb.QueryInput{
		TableName:                 &d.eventstoreTable,
		IndexName:                 eSIndex,
		Limit:                     &limit,
		KeyConditionExpression:    keyCond,
		ExpressionAttributeValues: exValue,
		ConsistentRead:            falseStrongRead,
	})

	if err != nil {
		return nil, err
	}

	if len(output.Items) == 0 {
		return nil, nil
	}

	snapshots := make([]*eventstore.EventMsg, 0, len(output.Items))
	if err = dynamodbattribute.UnmarshalListOfMaps(output.Items, &snapshots); err != nil {
		return nil, err
	}

	return snapshots, nil
}

func (d *DynamoDBEventStore) GetEventsByAggregateType(aggType string, seq, limit int64) ([]*eventstore.EventMsg, error) {
	keyCond := getByAggregateTypeCond
	exValue := map[string]*dynamodb.AttributeValue{
		getByAggregateTypeKV: &dynamodb.AttributeValue{S: &aggType},
	}

	if seq > 0 {
		exValue[seqKV] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(seq, 10))}
		keyCond = getByAggregateTypeWithTimeCond
	}

	output, err := d.db.Query(&dynamodb.QueryInput{
		TableName:                 &d.eventstoreTable,
		IndexName:                 bSIndex,
		Limit:                     &limit,
		KeyConditionExpression:    keyCond,
		ExpressionAttributeValues: exValue,
		ConsistentRead:            falseStrongRead,
	})

	if err != nil {
		return nil, err
	}

	if len(output.Items) == 0 {
		return nil, nil
	}

	snapshots := make([]*eventstore.EventMsg, 0, len(output.Items))
	if err = dynamodbattribute.UnmarshalListOfMaps(output.Items, &snapshots); err != nil {
		return nil, err
	}

	return snapshots, nil
}

func (d *DynamoDBEventStore) GetSnapshot(aggID string) (*eventstore.SnapshotMsg, error) {
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

	snapshot := &eventstore.SnapshotMsg{}
	if err = dynamodbattribute.UnmarshalMap(output.Item, snapshot); err != nil {
		return nil, err
	}

	return snapshot, nil
}

func (d *DynamoDBEventStore) GetAggregate(aggID string) (*eventstore.AggregateMsg, error) {
	output, err := d.db.GetItem(&dynamodb.GetItemInput{
		TableName:      &d.aggregateTable,
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

	aggmsg := &eventstore.AggregateMsg{}
	if err = dynamodbattribute.UnmarshalMap(output.Item, aggmsg); err != nil {
		return nil, err
	}

	return aggmsg, nil
}

func (d *DynamoDBEventStore) Save(events []*eventstore.EventMsg, aggmsg *eventstore.AggregateMsg) error {
	var err error
	var snapshotReq map[string]*dynamodb.AttributeValue
	snapshotReq, err = dynamodbattribute.MarshalMap(aggmsg)
	if err != nil {
		return err
	}

	var putES []*dynamodb.TransactWriteItem
	var payloadReq map[string]*dynamodb.AttributeValue

	if aggmsg != nil {
		putES = make([]*dynamodb.TransactWriteItem, 0, len(events)+1)
		putES = append(putES, &dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				TableName:           &d.aggregateTable,
				ConditionExpression: saveSnapCond,
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					seqKV: &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(aggmsg.TimeSeq, 10))},
				},
				Item: snapshotReq,
			},
		})
	} else {
		putES = make([]*dynamodb.TransactWriteItem, 0, len(events))
	}

	for i := 0; i < len(events); i++ {
		payloadReq, err = dynamodbattribute.MarshalMap(events[i])
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
