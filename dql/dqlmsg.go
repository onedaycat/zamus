package dql

//noinspection GoNameStartsWithPackageName
type DQLErrors = []*DQLError

//noinspection GoNameStartsWithPackageName
type DQLError struct {
    Message string      `json:"message"`
    Cause   string      `json:"cause"`
    Input   interface{} `json:"input"`
    Stacks  []string    `json:"stacks"`
}

//noinspection GoNameStartsWithPackageName
type DQLMsg struct {
    ID             string      `json:"id"`
    Service        string      `json:"service"`
    Time           int64       `json:"time"`
    Version        string      `json:"version"`
    LambdaFunction string      `json:"lambdaFunction"`
    EventMsgs      []byte      `json:"eventMsgs"`
    Errors         []*DQLError `json:"errors"`
}
