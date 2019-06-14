package lambda

import (
    "context"
    "encoding/json"
    "fmt"
    "reflect"
    "time"

    "github.com/aws/aws-lambda-go/lambda/messages"
    "github.com/aws/aws-lambda-go/lambdacontext"
    "github.com/onedaycat/errors"
)

type Function struct {
    handle Handle
}

func (fn *Function) Ping(req *messages.PingRequest, response *messages.PingResponse) error {
    *response = messages.PingResponse{}
    return nil
}

func (fn *Function) Invoke(req *messages.InvokeRequest, response *messages.InvokeResponse) error {
    defer panicResponse(req, response)

    deadline := time.Unix(req.Deadline.Seconds, req.Deadline.Nanos).UTC()
    invokeContext, cancel := context.WithDeadline(context.Background(), deadline)
    defer cancel()

    lc := &lambdacontext.LambdaContext{
        AwsRequestID:       req.RequestId,
        InvokedFunctionArn: req.InvokedFunctionArn,
        Identity: lambdacontext.CognitoIdentity{
            CognitoIdentityID:     req.CognitoIdentityId,
            CognitoIdentityPoolID: req.CognitoIdentityPoolId,
        },
    }
    if len(req.ClientContext) > 0 {
        if err := json.Unmarshal(req.ClientContext, &lc.ClientContext); err != nil {
            response.Error = lambdaErrorResponse(err)
            return nil
        }
    }
    invokeContext = lambdacontext.NewContext(invokeContext, lc)

    invokeContext = context.WithValue(invokeContext, "x-amzn-trace-id", req.XAmznTraceId)

    output, err := fn.handle.Invoke(invokeContext, req.Payload)
    if err != nil {
        response.Error = lambdaErrorResponse(err)
        return nil
    }
    if output == nil {
        response.Payload = nil
        return nil
    }

    payload, err := jsonfast.Marshal(output)
    if err != nil {
        response.Error = lambdaErrorResponse(err)
        return nil
    }

    response.Payload = payload
    return nil
}

func lambdaErrorResponse(invokeError error) *messages.InvokeResponse_Error {
    xerr, ok := invokeError.(errors.Error)
    if ok {
        return &messages.InvokeResponse_Error{
            Message:    xerr.Error(),
            Type:       xerr.GetType(),
            StackTrace: convertStacktace(xerr),
        }
    }

    return &messages.InvokeResponse_Error{
        Message: invokeError.Error(),
        Type:    getErrorType(invokeError),
    }
}

func getErrorType(err interface{}) string {
    errorType := reflect.TypeOf(err)
    if errorType.Kind() == reflect.Ptr {
        return errorType.Elem().Name()
    }
    return errorType.Name()
}

func panicResponse(req *messages.InvokeRequest, response *messages.InvokeResponse) {
    if r := recover(); r != nil {
        switch cause := r.(type) {
        case errors.Error:
            response.Error = &messages.InvokeResponse_Error{
                Type:       cause.GetType(),
                Message:    cause.Error(),
                StackTrace: convertStacktace(cause),
                ShouldExit: true,
            }
        case error:
            errType := getErrorType(cause)
            response.Error = &messages.InvokeResponse_Error{
                Type:       errType,
                Message:    fmt.Sprintf("%s: %s", errType, cause.Error()),
                ShouldExit: true,
            }
        default:
            errType := getErrorType(cause)
            response.Error = &messages.InvokeResponse_Error{
                Type:       errType,
                Message:    fmt.Sprintf("%s: %v", errType, cause),
                ShouldExit: true,
            }
        }
    }
}

func convertStacktace(err errors.Error) []*messages.InvokeResponse_Error_StackFrame {
    stack := err.GetStacktrace()
    if stack == nil {
        return nil
    }

    if len(stack) == 0 {
        return nil
    }

    stackFrams := make([]*messages.InvokeResponse_Error_StackFrame, len(stack))
    for i, frame := range stack {
        stackFrams[i] = &messages.InvokeResponse_Error_StackFrame{
            Path:  frame.Filename,
            Line:  int32(frame.Lineno),
            Label: frame.Function,
        }
    }

    return stackFrams
}
