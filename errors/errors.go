package errors

import (
    jsoniter "github.com/json-iterator/go"
    "github.com/onedaycat/errors"
)

type Input = errors.Input

const lambdaErrorType = "LambdaError"

var (
    ErrBatchRequestExceed   = errors.InternalError("ErrBatchRequestExceed", "Batch request more than 10 requests")
    ErrBatchMissResult      = errors.InternalError("ErrBatchMissResult", "Some of result in batch are nil")
    ErrUnableParseRequest   = errors.InternalError("ErrUnableParseRequest", "Unable to parse request")
    ErrUnablePublishKinesis = errors.InternalError("ErrUnablePublishKinesis", "Unable to publish kinesis stream")
    ErrUnknown              = errors.InternalError("ErrUnknown", "Unknown error")

    ErrVersionInconsistency     = errors.BadRequest("ErrVersionInconsistency", "Version is inconsistency")
    ErrEncodingNotSupported     = errors.InternalError("ErrEncodingNotSupported", "Unable unmarshal payload, unsupported encoding")
    ErrInvalidVersionNotAllowed = errors.InternalError("ErrInvalidVersionNotAllowed", "Event sequence should not be 0")
    ErrNoAggregateID            = errors.InternalError("ErrNoAggregateID", "No aggregate id in aggregate root")
    ErrUnableGetEventStore      = errors.InternalError("ErrUnableGetEventStore", "Unable to get")
    ErrUnableSaveEventStore     = errors.InternalError("ErrUnableSaveEventStore", "Unable to save")
    ErrUnableSaveDQLMessages    = errors.InternalError("ErrUnableSaveDQLMessages", "Unable to save DQL messages")
    ErrUnableInvokeFunction     = errors.InternalError("ErrUnableInvokeFunction", "Unable to invoke function")
    ErrUnableGetState           = errors.InternalError("ErrUnableGetState", "Unable get saga state")
    ErrUnableGetDQLMsg          = errors.InternalError("ErrUnableGetDQLMsg", "Unable get DQL message")
    ErrUnableSaveState          = errors.InternalError("ErrUnableSaveState", "Unable save saga state")
    ErrNoStateAction            = errors.InternalError("ErrNoStateAction", "No state action")
    ErrStateNotFound            = errors.InternalError("ErrStateNotFound", "Saga State not found")
    ErrDQLMsgNotFound           = errors.InternalError("ErrDQLMsgNotFound", "DQL message not found")
    ErrUnablePublishEvent       = errors.InternalError("ErrUnablePublishEvent", "Unable publish event")
    ErrEventProtoNotRegistered  = errors.InternalError("ErrEventProtoNotRegistered", "Event protobuf not registered yet")
)

const (
    ErrPanicCode            = "PanicError"
    ErrInternalErrorCode    = "InternalError"
    ErrInvalidRequestCode   = "InvalidRequest"
    ErrValidateErrorCode    = "ValidateError"
    ErrPermissionDeniedCode = "PermissionDenied"
    ErrTimeoutCode          = "TimeoutError"
    ErrUnauthorizedCode     = "Unauthorized"
    ErrUnavailableCode      = "Unavailable"
    ErrNotImplementCode     = "NotImplement"
    ErrUnableUnmarshalCode  = "UnableUnmarshal"
    ErrUnableMarshalCode    = "UnableMarshal"
    ErrUnableEncodeCode     = "UnableEncode"
    ErrUnableDecodeCode     = "UnableDecode"
    ErrUnableApplyEventCode = "UnableApplyEvent"
)

var (
    ErrPanic            = errors.InternalError(ErrPanicCode, "Panic Error").WithPanic()
    ErrInternalError    = errors.InternalError(ErrInternalErrorCode, "Internal error")
    ErrInvalidRequest   = errors.BadRequest(ErrInvalidRequestCode, "Invalid request")
    ErrValidateError    = errors.BadRequest(ErrValidateErrorCode, "Validation error")
    ErrPermissionDenied = errors.Forbidden(ErrPermissionDeniedCode, "You don't a permission to access this operation")
    ErrTimeout          = errors.Timeout(ErrTimeoutCode, "The operation is timeout")
    ErrUnauthorized     = errors.Unauthorized(ErrUnauthorizedCode, "The authorization is required")
    ErrUnavailable      = errors.Unavailable(ErrUnavailableCode, "This operation is unavailable")
    ErrNotImplement     = errors.NotImplement(ErrNotImplementCode, "This operation is not implemented")
    ErrUnableUnmarshal  = errors.InternalError(ErrUnableUnmarshalCode, "Unable to unmarshal")
    ErrUnableMarshal    = errors.InternalError(ErrUnableMarshalCode, "Unable to marshal")
    ErrUnableEncode     = errors.InternalError(ErrUnableEncodeCode, "Unable to encode")
    ErrUnableDecode     = errors.InternalError(ErrUnableDecodeCode, "Unable to decode")
    ErrUnableApplyEvent = errors.InternalError(ErrUnableApplyEventCode, "Unable to apply event")
)

func ErrFunctionNotFound(fn string) errors.Error {
    return errors.BadRequestf("ErrFunctionNotFound", "%s function not found", fn)
}

func ErrNextStateNotFound(state string) errors.Error {
    return errors.InternalErrorf("ErrNextStateNotFound", "Cannot go next state, %s state is not found", state)
}

func ErrorByCode(err errors.Error) errors.Error {
    switch err.GetCode() {
    case ErrPanicCode:
        return ErrPanic
    case ErrInternalErrorCode:
        return ErrInternalError
    case ErrInvalidRequestCode:
        return ErrInvalidRequest
    case ErrPermissionDeniedCode:
        return ErrPermissionDenied
    case ErrTimeoutCode:
        return ErrTimeout
    case ErrUnauthorizedCode:
        return ErrUnauthorized
    case ErrUnavailableCode:
        return ErrUnavailable
    case ErrNotImplementCode:
        return ErrNotImplement
    case ErrUnableUnmarshalCode:
        return ErrUnableUnmarshal
    case ErrUnableMarshalCode:
        return ErrUnableMarshal
    case ErrUnableEncodeCode:
        return ErrUnableEncode
    case ErrUnableDecodeCode:
        return ErrUnableDecode
    }

    return err
}

var json = jsoniter.ConfigFastest

func ToLambdaError(err errors.Error) error {
    if err == nil {
        return nil
    }

    data, _ := json.Marshal(err)

    return &LambdaError{msg: string(data)}
}

func ParseLambdaError(payload []byte) (errors.Error, errors.Error) {
    lbErr := &lambdaErrorResponse{}
    err := json.Unmarshal(payload, lbErr)
    if err != nil {
        return nil, ErrUnableUnmarshal.WithCaller().WithCause(err).WithInput(payload)
    }

    if lbErr.ErrType == lambdaErrorType {
        appErr := &errors.AppError{}
        if err := json.Unmarshal([]byte(lbErr.ErrMessage), appErr); err != nil {
            return nil, ErrUnableUnmarshal.WithCaller().WithCause(err).WithInput(payload)
        }

        return ErrorByCode(appErr), nil
    }

    return errors.InternalError(lbErr.ErrType, string(lbErr.ErrMessage)), nil
}

type lambdaErrorResponse struct {
    ErrMessage string `json:"errorMessage"`
    ErrType    string `json:"errorType"`
}

type LambdaError struct {
    msg string
}

func (e *LambdaError) Error() string {
    return e.msg
}
