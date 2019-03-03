package dql_test

import (
	"context"
	"testing"

	"github.com/onedaycat/zamus/common/random"
	"github.com/onedaycat/zamus/dql"
	"github.com/onedaycat/zamus/dql/mocks"
	"github.com/onedaycat/zamus/errors"
	"github.com/stretchr/testify/require"
)

func TestDQL(t *testing.T) {
	storage := &mocks.Storage{}
	errStack := errors.ErrEncodingNotSupported.WithCaller().StackStrings()

	d := dql.New(storage, 3, "srv1", "fn1", "1.0.0")

	ok := d.Retry()
	require.True(t, ok)
	require.Equal(t, 2, d.Remain)
	e1 := random.EventMsg().Build()
	d.AddEventMsgError(e1, errStack)

	ok = d.Retry()
	require.True(t, ok)
	require.Equal(t, 1, d.Remain)
	e2 := random.EventMsg().Build()
	d.AddEventMsgError(e2, errStack)

	ok = d.Retry()
	require.False(t, ok)
	require.Equal(t, 0, d.Remain)
	e3 := random.EventMsg().Build()
	d.AddEventMsgError(e3, errStack)

	ctx := context.Background()
	storage.On("MultiSave", ctx, d.DQLMsgs).Return(nil)
	err := d.Save(ctx)

	require.NoError(t, err)
	require.Equal(t, 3, d.Remain)
	require.Len(t, d.DQLMsgs, 0)
}
