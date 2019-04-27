package dynamodb

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/onedaycat/errors"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/onedaycat/zamus/event"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/internal/common/ptr"
)

const (
	hashKeyK   = "hk"
	rangeKeyK  = "rk"
	hashKeyKV  = ":hk"
	rangeKeyKV = ":rk"
	//emptyStr      = ""
)

var (
	//saveSnapCond        = ptr.String("attribute_not_exists(s) or s < :s")
	//snapshotCond        = ptr.String("a=:a")
	//snapshotCondWithVer = ptr.String("a=:a and v = :v")
	saveCond        = ptr.String("attribute_not_exists(rk)")
	getCond         = ptr.String("hk = :hk")
	getCondWithTime = ptr.String("hk = :hk and rk > :rk")
	falseStrongRead = ptr.Bool(false)
)

type EventStoreStorage struct {
	db              *dynamodb.DynamoDB
	eventstoreTable string
	snapshotTable   string
}

func New(db *dynamodb.DynamoDB, eventstoreTable, snapshotTable string) *EventStoreStorage {
	return &EventStoreStorage{
		db:              db,
		eventstoreTable: eventstoreTable,
		snapshotTable:   snapshotTable,
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
					"rk": {N: output.Items[i]["rk"].N},
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
					"hk": {S: output.Items[i]["hk"].S},
					"rk": {N: output.Items[i]["rk"].N},
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

func (d *EventStoreStorage) CreateSchema(enableStream bool) error {
	_, err := d.db.CreateTable(&dynamodb.CreateTableInput{
		BillingMode: ptr.String("PAY_PER_REQUEST"),
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled:  ptr.Bool(enableStream),
			StreamViewType: ptr.String("NEW_IMAGE"),
		},
		TableName: &d.eventstoreTable,
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: ptr.String("hk"),
				AttributeType: ptr.String("S"),
			},
			{
				AttributeName: ptr.String("rk"),
				AttributeType: ptr.String("N"),
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

	_, err = d.db.CreateTable(&dynamodb.CreateTableInput{
		BillingMode: ptr.String("PAY_PER_REQUEST"),
		TableName:   &d.snapshotTable,
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: ptr.String("hk"),
				AttributeType: ptr.String("S"),
			},
			{
				AttributeName: ptr.String("rk"),
				AttributeType: ptr.String("N"),
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

func (d *EventStoreStorage) GetEvents(ctx context.Context, aggID string, seq int64) (event.Msgs, errors.Error) {
	keyCond := getCond
	exValue := map[string]*dynamodb.AttributeValue{
		hashKeyKV: {S: &aggID},
	}

	if seq > 0 {
		exValue[rangeKeyKV] = &dynamodb.AttributeValue{N: ptr.String(strconv.FormatInt(seq, 10))}
		keyCond = getCondWithTime
	}

	msgs := make(event.Msgs, 0, 100)
	err := d.db.QueryPagesWithContext(ctx, &dynamodb.QueryInput{
		TableName:                 &d.eventstoreTable,
		KeyConditionExpression:    keyCond,
		ExpressionAttributeValues: exValue,
		ConsistentRead:            falseStrongRead,
	}, func(output *dynamodb.QueryOutput, lastPage bool) bool {
		if len(output.Items) == 0 {
			return false
		}

		submsgs := make(event.Msgs, 0, len(output.Items))
		_ = dynamodbattribute.UnmarshalListOfMaps(output.Items, &submsgs)
		msgs = append(msgs, submsgs...)

		return !lastPage
	})

	if err != nil {
		return nil, appErr.ErrUnableGetEventStore.WithCause(err).WithCaller().WithInput(errors.Input{
			"aggID": aggID,
			"seq":   seq,
		})
	}

	if len(msgs) == 0 {
		return nil, nil
	}

	return msgs, nil
}

func (d *EventStoreStorage) GetSnapshot(ctx context.Context, aggID string, version int) (*eventstore.Snapshot, errors.Error) {
	if version == 0 {
		return nil, nil
	}

	output, err := d.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName:      &d.snapshotTable,
		ConsistentRead: falseStrongRead,
		Key: map[string]*dynamodb.AttributeValue{
			hashKeyK:  {S: &aggID},
			rangeKeyK: {N: ptr.String(strconv.Itoa(version))},
		},
	})
	if err != nil {
		return nil, appErr.ErrUnableGetEventStore.WithCause(err).WithCaller().WithInput(aggID)
	}

	if len(output.Item) == 0 {
		return nil, nil
	}

	snapshot := &eventstore.Snapshot{}
	if err = dynamodbattribute.UnmarshalMap(output.Item, snapshot); err != nil {
		return nil, appErr.ErrUnableGetEventStore.WithCause(err).WithCaller().WithInput(errors.Input{
			"aggID":   aggID,
			"version": version,
		})
	}

	return snapshot, nil
}

func (d *EventStoreStorage) Save(ctx context.Context, msgs event.Msgs, snapshot *eventstore.Snapshot) errors.Error {
	if err := d.saveEvents(ctx, msgs); err != nil {
		return err
	}

	return d.saveSnapshot(ctx, snapshot)
}

func (d *EventStoreStorage) saveEvents(ctx context.Context, msgs event.Msgs) errors.Error {
	var err error

	var payloadReq map[string]*dynamodb.AttributeValue
	for i := 0; i < len(msgs); i++ {
		payloadReq, err = dynamodbattribute.MarshalMap(msgs[i])
		if err != nil {
			return appErr.ErrUnableSaveEventStore.WithCause(err).WithCaller()
		}

		payloadReq["hk"] = &dynamodb.AttributeValue{S: &msgs[i].AggID}
		payloadReq["rk"] = &dynamodb.AttributeValue{N: ptr.String(strconv.FormatInt(msgs[i].Seq, 10))}
		_, err = d.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
			TableName:           ptr.String(d.eventstoreTable),
			ConditionExpression: saveCond,
			Item:                payloadReq,
		})

		if err != nil {
			aerr := err.(awserr.Error)
			if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return appErr.ErrVersionInconsistency.WithCaller().WithInput(payloadReq)
			}

			return appErr.ErrUnableSaveEventStore.WithCause(err).WithCaller()
		}
	}

	return nil
}

func (d *EventStoreStorage) saveSnapshot(ctx context.Context, snapshot *eventstore.Snapshot) errors.Error {
	if snapshot == nil {
		return nil
	}

	var snapshotReq map[string]*dynamodb.AttributeValue
	var xerr error
	snapshotReq, xerr = dynamodbattribute.MarshalMap(snapshot)
	if xerr != nil {
		return appErr.ErrUnableSaveEventStore.WithCause(xerr).WithCaller()
	}

	snapshotReq["hk"] = &dynamodb.AttributeValue{S: &snapshot.AggID}
	snapshotReq["rk"] = &dynamodb.AttributeValue{N: ptr.String(strconv.Itoa(snapshot.Version))}
	_, xerr = d.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: &d.snapshotTable,
		Item:      snapshotReq,
	})

	if xerr != nil {
		aerr := xerr.(awserr.Error)
		if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return appErr.ErrVersionInconsistency.WithCaller().WithInput(snapshotReq)
		}

		return appErr.ErrUnableSaveEventStore.WithCause(xerr).WithCaller()
	}

	return nil
}
