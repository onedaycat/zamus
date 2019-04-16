package service

import (
    "context"

    "github.com/onedaycat/errors"
    appErr "github.com/onedaycat/zamus/errors"
    "github.com/onedaycat/zamus/internal/common"
)

func (h *ServiceHandler) doMergeHandler(info *handlerinfo, ctx context.Context, req *Request) (err errors.Error) {
    defer h.recovery(ctx, req, &err)
    err = info.mergeHandler(ctx, req, info.batchResult[:len(info.reqs)])
    if err != nil {
        for _, errHandler := range h.errHandlers {
            errHandler(ctx, req, err)
        }
        return err
    }

    return nil
}

func (h *ServiceHandler) BatchHandle(ctx context.Context, reqs []*Request) BatchResults {
    if reqs == nil {
        return nil
    }

    // CleanUp
    for _, handler := range h.handlers {
        if len(handler.reqs) > 0 {
            handler.reqs = handler.reqs[:0]
        }
    }

    for i, req := range reqs {
        _, ok := h.handlers[req.Method]
        if !ok {
            h.req.batchResult[i].Error = appErr.ErrFunctionNotFound(req.Method).(*errors.AppError)
            continue
        }

        h.handlers[req.Method].reqs = append(h.handlers[req.Method].reqs, req)
    }

    for _, handler := range h.handlers {
        if handler.mergeHandler != nil && len(handler.reqs) > 0 {
            reqIndexs := handler.mergeReq(h.req.req)
            err := h.doMergeHandler(handler, ctx, h.req.req)
            if err != nil {
                for _, reqIndex := range reqIndexs {
                    h.req.batchResult[reqIndex].Error = err.(*errors.AppError)
                }
            } else {
                for i, index := range reqIndexs {
                    h.req.batchResult[index] = handler.batchResult[i]
                }
            }
        } else if handler.mergeHandler == nil && len(handler.reqs) > 0 {
            for _, req := range handler.reqs {
                result, err := h.runHandler(handler, ctx, req)
                if err != nil {
                    h.req.batchResult[req.index].Error = err.(*errors.AppError)
                } else {
                    h.req.batchResult[req.index].Data = result
                }
            }
        }
    }

    isThrow := false
    for i := 0; i < h.req.totalIndex; i++ {
        if h.req.batchResult[i] == nil {
            isThrow = true
            h.req.batchResult[i].Error = appErr.ErrBatchMissResult
        }
    }

    if isThrow {
        for _, errHandler := range h.errHandlers {
            errHandler(ctx, h.req.reqs[0], appErr.ErrBatchMissResult.WithCaller().WithInput(h.req.reqs))
        }
        return h.req.batchResult
    }

    return h.req.batchResult
}

func (h *ServiceHandler) RunBatch(ctx context.Context, reqs []*Request) BatchResults {
    reqByte, err := common.MarshalJSON(reqs)
    if err != nil {
        panic(err)
    }

    resByte, _ := h.Invoke(ctx, reqByte)

    if err := common.UnmarshalJSON(resByte, &h.req.batchResult); err != nil {
        panic(err)
    }

    return h.req.batchResult
}
