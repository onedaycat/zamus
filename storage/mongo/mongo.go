package mongo

// import (
// 	"context"
// 	"strings"

// 	"github.com/mongodb/mongo-go-driver/bson"
// 	"github.com/mongodb/mongo-go-driver/mongo"
// 	"github.com/mongodb/mongo-go-driver/mongo/options"
// 	"github.com/mongodb/mongo-go-driver/x/bsonx"
// 	"github.com/onedaycat/gocqrs"
// )

// var (
// 	getOpts           = options.Find().SetSort(bson.D{{xid, 1}})
// 	getByVersionOpts  = options.Find().SetSort(bson.D{{xid, 1}})
// 	getByTimeOpts     = options.Find().SetSort(bson.D{{xid, 1}})
// 	upsertOpts        = options.Replace().SetUpsert(true)
// 	xid               = "_id"
// 	emptyStr          = ""
// 	errDupCode        = 11000
// 	insertManyOrdered = options.InsertMany().SetOrdered(true)
// )

// type MongoEventStore struct {
// 	client   *mongo.Client
// 	db       *mongo.Database
// 	event    *mongo.Collection
// 	snapshot *mongo.Collection
// }

// func NewMongoEventStore(client *mongo.Client, db string) *MongoEventStore {
// 	return &MongoEventStore{
// 		client:   client,
// 		db:       client.Database(db),
// 		event:    client.Database(db).Collection("event"),
// 		snapshot: client.Database(db).Collection("snapshot"),
// 	}
// }

// func (m *MongoEventStore) DropSchema() {
// 	ctx := context.Background()
// 	m.event.Drop(ctx)
// 	m.snapshot.Drop(ctx)
// }

// func (m *MongoEventStore) CreateSchema() error {
// 	ctx := context.Background()
// 	result := m.db.RunCommand(
// 		ctx,
// 		bsonx.Doc{{"create", bsonx.String("event")}},
// 	)
// 	if err := result.Err(); err != nil && !strings.Contains(err.Error(), "NamespaceExists") {
// 		return err
// 	}

// 	result = m.db.RunCommand(
// 		ctx,
// 		bsonx.Doc{{"create", bsonx.String("snapshot")}},
// 	)
// 	if err := result.Err(); err != nil && !strings.Contains(err.Error(), "NamespaceExists") {
// 		return err
// 	}

// 	if _, err := m.event.Indexes().CreateMany(ctx, []mongo.IndexModel{
// 		{
// 			Options: options.Index().
// 				SetName("aggAsc").
// 				SetUnique(true),
// 			Keys: bsonx.Doc{
// 				{"a", bsonx.Int32(1)},
// 				{"v", bsonx.Int32(1)},
// 				{"t", bsonx.Int32(1)},
// 			},
// 		},
// 		{
// 			Options: options.Index().
// 				SetName("aggTypeAsc").
// 				SetUnique(true),
// 			Keys: bsonx.Doc{
// 				{"b", bsonx.Int32(1)},
// 				{"v", bsonx.Int32(1)},
// 				{"t", bsonx.Int32(1)},
// 			},
// 		},
// 		{
// 			Options: options.Index().
// 				SetName("eventTypeAsc").
// 				SetUnique(true),
// 			Keys: bsonx.Doc{
// 				{"e", bsonx.Int32(1)},
// 				{"v", bsonx.Int32(1)},
// 				{"t", bsonx.Int32(1)},
// 			},
// 		},
// 	}); err != nil {
// 		return err
// 	}

// 	_, err := m.snapshot.Indexes().CreateMany(ctx, []mongo.IndexModel{
// 		{
// 			Options: options.Index().
// 				SetName("aggTypeAsc").
// 				SetUnique(true),
// 			Keys: bsonx.Doc{
// 				{"b", bsonx.Int32(1)},
// 				{"_id", bsonx.Int32(-1)},
// 			},
// 		},
// 	})

// 	return err
// }

// func (m *MongoEventStore) Get(id string, time int64) ([]*gocqrs.EventMessage, error) {
// 	ctx := context.Background()

// 	cursor, err := m.event.Find(ctx,
// 		bson.D{
// 			{"a", id},
// 			{"t", bson.D{{"$gt", time}}},
// 		},
// 		getOpts,
// 	)
// 	if err != nil {
// 		return nil, err
// 	}

// 	ems := []*gocqrs.EventMessage{}
// 	for cursor.Next(ctx) {
// 		em := gocqrs.EventMessage{}
// 		cursor.Decode(&em)

// 		ems = append(ems, &em)
// 	}

// 	return ems, nil
// }

// func (m *MongoEventStore) GetByEventType(eventType gocqrs.EventType, time int64) ([]*gocqrs.EventMessage, error) {
// 	ctx := context.Background()

// 	cursor, err := m.event.Find(ctx, bson.D{
// 		{"e", eventType},
// 		{"t", bson.D{{"$gt", time}}},
// 	}, getOpts)
// 	if err != nil {
// 		return nil, err
// 	}

// 	ems := []*gocqrs.EventMessage{}
// 	for cursor.Next(ctx) {
// 		em := gocqrs.EventMessage{}
// 		cursor.Decode(&em)

// 		ems = append(ems, &em)
// 	}

// 	return ems, nil
// }

// func (m *MongoEventStore) GetByAggregateType(aggType gocqrs.AggregateType, time int64) ([]*gocqrs.EventMessage, error) {
// 	ctx := context.Background()

// 	cursor, err := m.event.Find(ctx, bson.D{
// 		{"b", aggType},
// 		{"t", bson.D{{"$gt", time}}},
// 	}, getOpts)
// 	if err != nil {
// 		return nil, err
// 	}

// 	ems := []*gocqrs.EventMessage{}
// 	for cursor.Next(ctx) {
// 		em := gocqrs.EventMessage{}
// 		cursor.Decode(&em)

// 		ems = append(ems, &em)
// 	}

// 	return ems, nil
// }

// func (m *MongoEventStore) GetSnapshot(id string) (*gocqrs.Snapshot, error) {
// 	ctx := context.Background()

// 	snapshot := &gocqrs.Snapshot{}

// 	err := m.snapshot.FindOne(ctx, bson.D{{xid, id}}).Decode(snapshot)
// 	if err != nil && err != mongo.ErrNoDocuments {
// 		return nil, err
// 	}

// 	if err == mongo.ErrNoDocuments {
// 		return nil, gocqrs.ErrNotFound
// 	}

// 	return snapshot, nil
// }

// func (m *MongoEventStore) BeginTx(fn func(ctx context.Context) error) error {
// 	ctx := context.Background()
// 	sess, err := m.client.StartSession()
// 	if err != nil {
// 		return err
// 	}
// 	defer sess.EndSession(ctx)

// 	err = mongo.WithSession(ctx, sess, func(sctx mongo.SessionContext) error {
// 		return fn(sctx)
// 	})

// 	return err
// }

// func (m *MongoEventStore) Save(ctx context.Context, payloads []*gocqrs.EventMessage, snapshot *gocqrs.Snapshot) error {
// 	docs := make([]interface{}, len(payloads))
// 	for i := 0; i < len(payloads); i++ {
// 		docs[i] = payloads[i]
// 	}

// 	_, err := m.event.InsertMany(ctx, docs, insertManyOrdered)
// 	if err != nil {
// 		aerr, ok := err.(mongo.BulkWriteException)
// 		if ok {
// 			for _, wrs := range aerr.WriteErrors {
// 				if wrs.Code == errDupCode {
// 					return gocqrs.ErrVersionInconsistency
// 				}
// 			}
// 		}

// 		return err
// 	}

// 	_, err = m.snapshot.ReplaceOne(ctx, bson.D{
// 		{xid, snapshot.ID},
// 	}, snapshot, upsertOpts)

// 	return err
// }

// func createToken(limit int, length int, events []*gocqrs.EventMessage) ([]*gocqrs.EventMessage, string) {
// 	if limit != 0 && length > limit {
// 		lastIndex := length - 1
// 		newEvents := events[:lastIndex]
// 		return newEvents, newEvents[lastIndex].ID
// 	}

// 	return events, emptyStr
// }
