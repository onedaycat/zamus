package query

import (
	"context"
	"testing"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/common"
	appErr "github.com/onedaycat/zamus/errors"
	"github.com/stretchr/testify/require"
)

type queryResult struct {
	result string
}

func newQueryResult() *queryResult {
	return &queryResult{"1"}
}

func (r *queryResult) Len() int {
	return 1
}

type queryResultList struct {
	result []string
}

func newQueryResultList() *queryResultList {
	return &queryResultList{[]string{"1", "2", "3"}}
}

func (r *queryResultList) Len() int {
	return len(r.result)
}

func TestNoQueryHandler(t *testing.T) {
	var query *Query

	h := NewHandler(&Config{
		SentryDNS: "test",
	})

	resp, err := h.Handle(context.Background(), query)

	require.Equal(t, appErr.ErrUnableParseQuery, err)
	require.Nil(t, resp)
}

func TestInvokeHandler(t *testing.T) {
	nF := 0
	nErr := 0

	query := &Query{
		Function: "q1",
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return newQueryResult(), nil
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Nil(t, err)
	require.Equal(t, newQueryResult(), resp)
	require.Equal(t, 1, nF)
	require.Equal(t, 0, nErr)
}

func TestQueryNotFoundHandler(t *testing.T) {
	query := &Query{
		Function: "q1",
	}

	h := NewHandler(&Config{})

	resp, err := h.Handle(context.Background(), query)

	require.Equal(t, appErr.ErrQueryNotFound("q1"), err)
	require.Nil(t, resp)
}

func TestInvokeHandlerError(t *testing.T) {
	nF := 0
	nErr := 0

	query := &Query{
		Function: "q1",
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return nil, appErr.ErrUnknown
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		require.Equal(t, appErr.ErrUnknown, err)
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Equal(t, appErr.ErrUnknown, err)
	require.Nil(t, resp)
	require.Equal(t, 1, nF)
	require.Equal(t, 1, nErr)
}

func TestInvokeHandlerPanic(t *testing.T) {
	nF := 0
	nErr := 0

	query := &Query{
		Function: "q1",
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		var q *Query
		_ = q.NBatchSources
		return newQueryResult(), nil
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		require.Equal(t, appErr.ErrInternalError, err)
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Equal(t, appErr.ErrInternalError, err)
	require.Nil(t, resp)
	require.Equal(t, 1, nF)
	require.Equal(t, 1, nErr)
}

func TestInvokeHandlerPanicString(t *testing.T) {
	spy := common.Spy()

	query := &Query{
		Function: "q1",
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		spy.Called("f")
		panic("hello")
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		require.Equal(t, appErr.ErrInternalError, err)
		spy.Called("fErr")
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Equal(t, appErr.ErrInternalError, err)
	require.Nil(t, resp)
	require.Equal(t, 1, spy.Count("f"))
	require.Equal(t, 1, spy.Count("fErr"))
}

func TestBatchInvokeHandler(t *testing.T) {
	nF := 0
	nErr := 0

	query := &Query{
		Function:      "q1",
		NBatchSources: 3,
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return newQueryResultList(), nil
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Nil(t, err)
	require.Equal(t, newQueryResultList(), resp)
	require.Equal(t, 1, nF)
	require.Equal(t, 0, nErr)
}

func TestBatchInvokeHandlerNBatchSourcesMisMatched(t *testing.T) {
	nF := 0
	nErr := 0

	query := &Query{
		Function:      "q1",
		NBatchSources: 2,
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return newQueryResultList(), nil
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Equal(t, appErr.ErrQueryResultSizeNotMatch, err)
	require.Nil(t, resp)
	require.Equal(t, 1, nF)
	require.Equal(t, 0, nErr)
}

func TestBatchInvokePreHandler(t *testing.T) {
	nF := 0
	nErr := 0
	nPre := 0

	query := &Query{
		Function:      "q1",
		NBatchSources: 3,
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return newQueryResultList(), nil
	}

	fPre := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nPre++
		return nil, nil
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f)
	h.PreHandlers(fPre, fPre)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Nil(t, err)
	require.Equal(t, newQueryResultList(), resp)
	require.Equal(t, 1, nF)
	require.Equal(t, 0, nErr)
	require.Equal(t, 2, nPre)
}

func TestBatchInvokePreHandlerError(t *testing.T) {
	nF := 0
	nErr := 0
	nPre := 0

	query := &Query{
		Function:      "q1",
		NBatchSources: 3,
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return newQueryResultList(), nil
	}

	fPre := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nPre++
		return nil, appErr.ErrUnknown
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f)
	h.PreHandlers(fPre, fPre)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Equal(t, appErr.ErrUnknown, err)
	require.Nil(t, resp)
	require.Equal(t, 0, nF)
	require.Equal(t, 1, nErr)
	require.Equal(t, 1, nPre)
}

func TestBatchInvokePreHandlerNBatchSourcesMisMatched(t *testing.T) {
	nF := 0
	nErr := 0
	nPre := 0

	query := &Query{
		Function:      "q1",
		NBatchSources: 2,
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return newQueryResultList(), nil
	}

	fPre := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nPre++
		return newQueryResultList(), nil
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f)
	h.PreHandlers(fPre, fPre)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Equal(t, appErr.ErrQueryResultSizeNotMatch, err)
	require.Nil(t, resp)
	require.Equal(t, 0, nF)
	require.Equal(t, 0, nErr)
	require.Equal(t, 1, nPre)
}

func TestBatchInvokePreHandlerReturn(t *testing.T) {
	nF := 0
	nErr := 0
	nPre := 0

	query := &Query{
		Function:      "q1",
		NBatchSources: 3,
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return nil, nil
	}

	fPre := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nPre++
		return newQueryResultList(), nil
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f)
	h.PreHandlers(fPre, fPre)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Nil(t, err)
	require.Equal(t, newQueryResultList(), resp)
	require.Equal(t, 0, nF)
	require.Equal(t, 0, nErr)
	require.Equal(t, 1, nPre)
}

func TestBatchInvokePostHandler(t *testing.T) {
	nF := 0
	nErr := 0
	nPost := 0

	query := &Query{
		Function:      "q1",
		NBatchSources: 3,
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return newQueryResultList(), nil
	}

	fPost := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nPost++
		return nil, nil
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f)
	h.PostHandlers(fPost, fPost)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Nil(t, err)
	require.Equal(t, newQueryResultList(), resp)
	require.Equal(t, 1, nF)
	require.Equal(t, 0, nErr)
	require.Equal(t, 2, nPost)
}

func TestBatchInvokePostHandlerError(t *testing.T) {
	nF := 0
	nErr := 0
	nPost := 0

	query := &Query{
		Function:      "q1",
		NBatchSources: 3,
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return newQueryResultList(), nil
	}

	fPost := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nPost++
		return nil, appErr.ErrUnknown
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f)
	h.PostHandlers(fPost, fPost)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Equal(t, appErr.ErrUnknown, err)
	require.Nil(t, resp)
	require.Equal(t, 1, nF)
	require.Equal(t, 1, nErr)
	require.Equal(t, 1, nPost)
}

func TestBatchInvokePostHandlerNBatchSourcesMisMatched(t *testing.T) {
	nF := 0
	nErr := 0
	nPost := 0

	query := &Query{
		Function:      "q1",
		NBatchSources: 3,
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return newQueryResultList(), nil
	}

	fPost := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nPost++
		list := newQueryResultList()
		list.result = list.result[:2]
		return list, nil
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f)
	h.PostHandlers(fPost, fPost)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Equal(t, appErr.ErrQueryResultSizeNotMatch, err)
	require.Nil(t, resp)
	require.Equal(t, 1, nF)
	require.Equal(t, 0, nErr)
	require.Equal(t, 1, nPost)
}

func TestBatchInvokePostHandlerReturn(t *testing.T) {
	nF := 0
	nErr := 0
	nPost := 0

	query := &Query{
		Function:      "q1",
		NBatchSources: 3,
	}

	postList := newQueryResultList()
	postList.result[0] = "10"

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return newQueryResultList(), nil
	}

	fPost := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nPost++
		return postList, nil
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f)
	h.PostHandlers(fPost, fPost)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Nil(t, err)
	require.Equal(t, postList, resp)
	require.Equal(t, 1, nF)
	require.Equal(t, 0, nErr)
	require.Equal(t, 1, nPost)
}

func TestBatchInvokePreHandlerEachHandlerError(t *testing.T) {
	nF := 0
	nErr := 0
	nPre := 0

	query := &Query{
		Function:      "q1",
		NBatchSources: 3,
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return newQueryResultList(), nil
	}

	fPre := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nPre++
		return nil, appErr.ErrUnknown
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f, fPre, fPre)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Equal(t, appErr.ErrUnknown, err)
	require.Nil(t, resp)
	require.Equal(t, 0, nF)
	require.Equal(t, 1, nErr)
	require.Equal(t, 1, nPre)
}

func TestBatchInvokePreHandlerEachHandlerNBatchSourcesMisMatched(t *testing.T) {
	nF := 0
	nErr := 0
	nPre := 0

	query := &Query{
		Function:      "q1",
		NBatchSources: 2,
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return newQueryResultList(), nil
	}

	fPre := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nPre++
		return newQueryResultList(), nil
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f, fPre, fPre)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Equal(t, appErr.ErrQueryResultSizeNotMatch, err)
	require.Nil(t, resp)
	require.Equal(t, 0, nF)
	require.Equal(t, 0, nErr)
	require.Equal(t, 1, nPre)
}

func TestBatchInvokePreHandlerEachHandlerReturn(t *testing.T) {
	nF := 0
	nErr := 0
	nPre := 0

	query := &Query{
		Function:      "q1",
		NBatchSources: 3,
	}

	f := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nF++
		return nil, nil
	}

	fPre := func(ctx context.Context, query *Query) (QueryResult, errors.Error) {
		nPre++
		return newQueryResultList(), nil
	}

	fErr := func(ctx context.Context, query *Query, err errors.Error) {
		nErr++
	}

	h := NewHandler(&Config{})
	h.RegisterQuery("q1", f, fPre, fPre)
	h.ErrorHandlers(fErr)

	resp, err := h.Handle(context.Background(), query)

	require.Nil(t, err)
	require.Equal(t, newQueryResultList(), resp)
	require.Equal(t, 0, nF)
	require.Equal(t, 0, nErr)
	require.Equal(t, 1, nPre)
}
