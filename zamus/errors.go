package zamus

import (
    "reflect"
)

//import (
//    "reflect"
//)
//
//type lambdaError struct {
//    ErrorMessage string `json:"errorMessage"`
//    ErrorType    string `json:"errorType"`
//}
//
//func NewLambdaErrorFrom(err error) *lambdaError {
//    switch e := err.(type) {
//    case errors.Error:
//        return &lambdaError{
//            ErrorMessage: e.Error(),
//            ErrorType:    e.GetCode(),
//        }
//    }
//
//    return &lambdaError{
//        ErrorType:    GetErrorType(err),
//        ErrorMessage: err.Error(),
//    }
//}
//
//func (e *lambdaError) GetCode() string {
//    return e.ErrorType
//}
//
//func (e *lambdaError) Error() string {
//    return e.ErrorMessage
//}
//
func GetErrorType(err interface{}) string {
    errorType := reflect.TypeOf(err)
    if errorType.Kind() == reflect.Ptr {
        return errorType.Elem().Name()
    }
    return errorType.Name()
}
