package service_test

import (
    "context"
    "testing"

    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/random"
    "github.com/stretchr/testify/require"
)

type BatchArgs struct {
    ID string `json:"id"`
}

type BatchArgsList []*BatchArgs

func (b BatchArgsList) GetMergeBatchResult(index int) interface{} {
    return b[index]
}

func TestBatchHandler(t *testing.T) {
    s := setupHandlerSuite()

    t.Run("Success Round 1", func(t *testing.T) {
        s.WithBatchHandler("f1", MODE_NORMAL)
        s.WithBatchHandler("f2", MODE_NORMAL)

        args := []*BatchArgs{
            {ID: "1"},
            {ID: "2"},
            {ID: "3"},
            {ID: "4"},
            {ID: "5"},
        }

        reqs := random.ServiceReqs().
            Add("f1", args[0], args[1], args[2], args[3], args[4]).
            Build()

        resList := s.h.RunBatch(context.Background(), reqs)

        require.Len(t, resList, 5)
        require.Equal(t, args[0], resList[0].Data)
        require.Equal(t, args[1], resList[1].Data)
        require.Equal(t, args[2], resList[2].Data)
        require.Equal(t, args[3], resList[3].Data)
        require.Equal(t, args[4], resList[4].Data)
        require.Nil(t, resList[0].Error)
        require.Nil(t, resList[1].Error)
        require.Nil(t, resList[2].Error)
        require.Nil(t, resList[3].Error)
        require.Nil(t, resList[4].Error)
        require.Equal(t, 5, s.spy.Count("f1"))
        require.Equal(t, 0, s.spy.Count("f2"))
        require.Equal(t, 0, s.spy.Count("err"))
    })

    t.Run("Success Round 2", func(t *testing.T) {
        s.WithBatchHandler("f1", MODE_NORMAL)
        s.WithBatchHandler("f2", MODE_NORMAL)

        args := []*BatchArgs{
            {ID: "1"},
            {ID: "2"},
            {ID: "3"},
            {ID: "4"},
            {ID: "5"},
        }

        reqs := random.ServiceReqs().
            Add("f1", args[0], args[1], args[2], args[3], args[4]).
            Add("f2", args[0], args[1], args[2], args[3], args[4]).
            Build()

        resList := s.h.RunBatch(context.Background(), reqs)

        require.Len(t, resList, 10)
        require.Equal(t, args[0], resList[0].Data)
        require.Equal(t, args[1], resList[1].Data)
        require.Equal(t, args[2], resList[2].Data)
        require.Equal(t, args[3], resList[3].Data)
        require.Equal(t, args[4], resList[4].Data)
        require.Equal(t, args[0], resList[5].Data)
        require.Equal(t, args[1], resList[6].Data)
        require.Equal(t, args[2], resList[7].Data)
        require.Equal(t, args[3], resList[8].Data)
        require.Equal(t, args[4], resList[9].Data)
        require.Nil(t, resList[0].Error)
        require.Nil(t, resList[1].Error)
        require.Nil(t, resList[2].Error)
        require.Nil(t, resList[3].Error)
        require.Nil(t, resList[4].Error)
        require.Nil(t, resList[5].Error)
        require.Nil(t, resList[6].Error)
        require.Nil(t, resList[7].Error)
        require.Nil(t, resList[8].Error)
        require.Nil(t, resList[9].Error)
        require.Equal(t, 10, s.spy.Count("f1"))
        require.Equal(t, 5, s.spy.Count("f2"))
        require.Equal(t, 0, s.spy.Count("err"))
    })

    t.Run("Error", func(t *testing.T) {
        s.WithBatchHandler("f1", MODE_ERROR)
        s.WithBatchHandler("f2", MODE_NORMAL)

        args := []*BatchArgs{
            {ID: "1"},
            {ID: "2"},
            {ID: "3"},
            {ID: "4"},
            {ID: "5"},
        }

        reqs := random.ServiceReqs().
            Add("f1", args[0], args[1], args[2], args[3], args[4]).
            Add("f2", args[0], args[1], args[2], args[3], args[4]).
            Build()

        resList := s.h.RunBatch(context.Background(), reqs)

        require.Len(t, resList, 10)
        require.Nil(t, resList[0].Data)
        require.Nil(t, resList[1].Data)
        require.Nil(t, resList[2].Data)
        require.Nil(t, resList[3].Data)
        require.Nil(t, resList[4].Data)
        require.Equal(t, args[0], resList[5].Data)
        require.Equal(t, args[1], resList[6].Data)
        require.Equal(t, args[2], resList[7].Data)
        require.Equal(t, args[3], resList[8].Data)
        require.Equal(t, args[4], resList[9].Data)
        require.Equal(t, appErr.ErrInternalError, resList[0].Error)
        require.Equal(t, appErr.ErrInternalError, resList[1].Error)
        require.Equal(t, appErr.ErrInternalError, resList[2].Error)
        require.Equal(t, appErr.ErrInternalError, resList[3].Error)
        require.Equal(t, appErr.ErrInternalError, resList[4].Error)
        require.Nil(t, resList[5].Error)
        require.Nil(t, resList[6].Error)
        require.Nil(t, resList[7].Error)
        require.Nil(t, resList[8].Error)
        require.Nil(t, resList[9].Error)
        require.Equal(t, 15, s.spy.Count("f1"))
        require.Equal(t, 10, s.spy.Count("f2"))
        require.Equal(t, 5, s.spy.Count("err"))
    })

    t.Run("No Req", func(t *testing.T) {
        s.WithBatchHandler("f1", MODE_NORMAL)
        s.WithBatchHandler("f2", MODE_NORMAL)

        res := s.h.BatchHandle(context.Background(), nil)

        require.Nil(t, res)
    })

    t.Run("NotFound", func(t *testing.T) {
        s.WithBatchHandler("f1", MODE_NORMAL)
        s.WithBatchHandler("f2", MODE_NORMAL)

        args := []*BatchArgs{
            {ID: "1"},
            {ID: "2"},
            {ID: "3"},
            {ID: "4"},
            {ID: "5"},
        }

        reqs := random.ServiceReqs().
            Add("f3", args[0], args[1], args[2], args[3], args[4]).
            Add("f4", args[0], args[1], args[2], args[3], args[4]).
            Build()

        res := s.h.RunBatch(context.Background(), reqs)

        require.Equal(t, appErr.ErrFunctionNotFound("f3"), res[0].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f3"), res[1].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f3"), res[2].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f3"), res[3].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f3"), res[4].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f4"), res[5].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f4"), res[6].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f4"), res[7].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f4"), res[8].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f4"), res[9].Error)
        require.Nil(t, res[0].Data)
        require.Nil(t, res[1].Data)
        require.Nil(t, res[2].Data)
        require.Nil(t, res[3].Data)
        require.Nil(t, res[4].Data)
        require.Nil(t, res[5].Data)
        require.Nil(t, res[6].Data)
        require.Nil(t, res[7].Data)
        require.Nil(t, res[8].Data)
        require.Nil(t, res[9].Data)
        require.Equal(t, 15, s.spy.Count("f1"))
        require.Equal(t, 10, s.spy.Count("f2"))
        require.Equal(t, 5, s.spy.Count("err"))
    })
}

func TestMergeHandler(t *testing.T) {
    s := setupHandlerSuite()

    t.Run("Success Round 1", func(t *testing.T) {
        s.WithMergeHandler("f1", MODE_NORMAL)
        s.WithBatchHandler("f2", MODE_NORMAL)

        args := []*BatchArgs{
            {ID: "1"},
            {ID: "2"},
            {ID: "3"},
            {ID: "4"},
            {ID: "5"},
        }

        reqs := random.ServiceReqs().
            Add("f1", args[0], args[1], args[2], args[3], args[4]).
            Build()

        resList := s.h.RunBatch(context.Background(), reqs)

        require.Len(t, resList, 5)
        require.Equal(t, args[0], resList[0].Data)
        require.Equal(t, args[1], resList[1].Data)
        require.Equal(t, args[2], resList[2].Data)
        require.Equal(t, args[3], resList[3].Data)
        require.Equal(t, args[4], resList[4].Data)
        require.Nil(t, resList[0].Error)
        require.Nil(t, resList[1].Error)
        require.Nil(t, resList[2].Error)
        require.Nil(t, resList[3].Error)
        require.Nil(t, resList[4].Error)
        require.Equal(t, 1, s.spy.Count("f1"))
        require.Equal(t, 0, s.spy.Count("f2"))
        require.Equal(t, 0, s.spy.Count("err"))
    })

    t.Run("Success Round 2", func(t *testing.T) {
        s.WithMergeHandler("f1", MODE_NORMAL)
        s.WithBatchHandler("f2", MODE_NORMAL)

        args := []*BatchArgs{
            {ID: "1"},
            {ID: "2"},
            {ID: "3"},
            {ID: "4"},
            {ID: "5"},
        }

        reqs := random.ServiceReqs().
            Add("f1", args[0], args[1], args[2], args[3], args[4]).
            Add("f2", args[0], args[1], args[2], args[3], args[4]).
            Build()

        resList := s.h.RunBatch(context.Background(), reqs)

        require.Len(t, resList, 10)
        require.Equal(t, args[0], resList[0].Data)
        require.Equal(t, args[1], resList[1].Data)
        require.Equal(t, args[2], resList[2].Data)
        require.Equal(t, args[3], resList[3].Data)
        require.Equal(t, args[4], resList[4].Data)
        require.Equal(t, args[0], resList[5].Data)
        require.Equal(t, args[1], resList[6].Data)
        require.Equal(t, args[2], resList[7].Data)
        require.Equal(t, args[3], resList[8].Data)
        require.Equal(t, args[4], resList[9].Data)
        require.Nil(t, resList[0].Error)
        require.Nil(t, resList[1].Error)
        require.Nil(t, resList[2].Error)
        require.Nil(t, resList[3].Error)
        require.Nil(t, resList[4].Error)
        require.Nil(t, resList[5].Error)
        require.Nil(t, resList[6].Error)
        require.Nil(t, resList[7].Error)
        require.Nil(t, resList[8].Error)
        require.Nil(t, resList[9].Error)
        require.Equal(t, 2, s.spy.Count("f1"))
        require.Equal(t, 5, s.spy.Count("f2"))
        require.Equal(t, 0, s.spy.Count("err"))
    })

    t.Run("Error", func(t *testing.T) {
        s.WithMergeHandler("f1", MODE_ERROR)
        s.WithMergeHandler("f2", MODE_NORMAL)

        args := []*BatchArgs{
            {ID: "1"},
            {ID: "2"},
            {ID: "3"},
            {ID: "4"},
            {ID: "5"},
        }

        reqs := random.ServiceReqs().
            Add("f1", args[0], args[1], args[2], args[3], args[4]).
            Add("f2", args[0], args[1], args[2], args[3], args[4]).
            Build()

        resList := s.h.RunBatch(context.Background(), reqs)

        require.Len(t, resList, 10)
        require.Nil(t, resList[0].Data)
        require.Nil(t, resList[1].Data)
        require.Nil(t, resList[2].Data)
        require.Nil(t, resList[3].Data)
        require.Nil(t, resList[4].Data)
        require.Equal(t, args[0], resList[5].Data)
        require.Equal(t, args[1], resList[6].Data)
        require.Equal(t, args[2], resList[7].Data)
        require.Equal(t, args[3], resList[8].Data)
        require.Equal(t, args[4], resList[9].Data)
        require.Equal(t, appErr.ErrInternalError, resList[0].Error)
        require.Equal(t, appErr.ErrInternalError, resList[1].Error)
        require.Equal(t, appErr.ErrInternalError, resList[2].Error)
        require.Equal(t, appErr.ErrInternalError, resList[3].Error)
        require.Equal(t, appErr.ErrInternalError, resList[4].Error)
        require.Nil(t, resList[5].Error)
        require.Nil(t, resList[6].Error)
        require.Nil(t, resList[7].Error)
        require.Nil(t, resList[8].Error)
        require.Nil(t, resList[9].Error)
        require.Equal(t, 3, s.spy.Count("f1"))
        require.Equal(t, 6, s.spy.Count("f2"))
        require.Equal(t, 1, s.spy.Count("err"))
    })

    t.Run("NotFound", func(t *testing.T) {
        s.WithMergeHandler("f1", MODE_NORMAL)
        s.WithMergeHandler("f2", MODE_NORMAL)

        args := []*BatchArgs{
            {ID: "1"},
            {ID: "2"},
            {ID: "3"},
            {ID: "4"},
            {ID: "5"},
        }

        reqs := random.ServiceReqs().
            Add("f3", args[0], args[1], args[2], args[3], args[4]).
            Add("f4", args[0], args[1], args[2], args[3], args[4]).
            Build()

        res := s.h.RunBatch(context.Background(), reqs)

        require.Equal(t, appErr.ErrFunctionNotFound("f3"), res[0].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f3"), res[1].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f3"), res[2].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f3"), res[3].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f3"), res[4].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f4"), res[5].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f4"), res[6].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f4"), res[7].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f4"), res[8].Error)
        require.Equal(t, appErr.ErrFunctionNotFound("f4"), res[9].Error)
        require.Nil(t, res[0].Data)
        require.Nil(t, res[1].Data)
        require.Nil(t, res[2].Data)
        require.Nil(t, res[3].Data)
        require.Nil(t, res[4].Data)
        require.Nil(t, res[5].Data)
        require.Nil(t, res[6].Data)
        require.Nil(t, res[7].Data)
        require.Nil(t, res[8].Data)
        require.Nil(t, res[9].Data)
        require.Equal(t, 3, s.spy.Count("f1"))
        require.Equal(t, 6, s.spy.Count("f2"))
        require.Equal(t, 1, s.spy.Count("err"))
    })
}
