// +build integration

package dynamodb

import (
    "context"
    "testing"
    "time"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/onedaycat/errors"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/internal/common/ptr"
    "github.com/onedaycat/zamus/random"
    "github.com/onedaycat/zamus/saga"
    "github.com/onedaycat/zamus/testdata/domain"
    "github.com/stretchr/testify/require"
)

var _db *SagaStore

func getDB() *SagaStore {
    if _db == nil {
        sess, err := session.NewSession(&aws.Config{
            Region:   ptr.String("ap-southeast-1"),
            Endpoint: ptr.String("http://localhost:8000"),
        })
        if err != nil {
            panic(err)
        }

        _db = New(dynamodb.New(sess), "gocqrs-eventstore-saga-dev")
        err = _db.CreateSchema(true)
        if err != nil {
            panic(err)
        }
    }

    return _db
}

func TestSaveAndGet(t *testing.T) {
    db := getDB()
    db.Truncate()

    now := time.Now().UTC()

    state := &saga.State{}
    state.Clear()

    state.ID = "state1"
    state.Name = "Test"
    state.Status = saga.SUCCESS
    state.Action = saga.COMPENSATE
    state.EventMsg = random.EventMsg().Event(&domain.StockItemCreated{Id: "1"}).Build()
    state.StartTime = now.Unix()
    state.LastTime = now.Unix()
    state.Compensate = true
    state.Data = []byte(`{"id":6}`)
    state.Steps = []*saga.Step{
        {
            Name:      "s1",
            Status:    saga.SUCCESS,
            Action:    saga.NEXT,
            Retried:   0,
            StepError: nil,
        },
        {
            Name:      "s2",
            Status:    saga.SUCCESS,
            Action:    saga.NEXT,
            Retried:   0,
            StepError: nil,
        },
        {
            Name:      "s3",
            Status:    saga.ERROR,
            Action:    saga.COMPENSATE,
            Retried:   0,
            StepError: errors.DumbError,
        },
        {
            Name:      "s2",
            Status:    saga.SUCCESS,
            Action:    saga.BACK,
            Retried:   0,
            StepError: nil,
        },
        {
            Name:      "s1",
            Status:    saga.SUCCESS,
            Action:    saga.BACK,
            Retried:   0,
            StepError: nil,
        },
    }

    err := db.Save(context.Background(), state)
    require.NoError(t, err)

    getState := &saga.State{}
    err = db.Get(context.Background(), state.ID, getState)
    require.NoError(t, err)
    require.Equal(t, getState, state)

    // NotFound
    getState = &saga.State{}
    err = db.Get(context.Background(), "xxxx", getState)
    //noinspection GoNilness
    require.Equal(t, appErr.ErrStateNotFound.Error(), err.Error())
    require.Empty(t, getState.ID)
}
