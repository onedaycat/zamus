package dlq

//noinspection GoNameStartsWithPackageName
type DLQErrors = []*DLQError

//noinspection GoNameStartsWithPackageName
type DLQError struct {
    Message string      `json:"message"`
    Cause   string      `json:"cause"`
    Input   interface{} `json:"input"`
    Stacks  []string    `json:"stacks"`
}

//noinspection GoNameStartsWithPackageName
type DLQMsg struct {
    ID             string      `json:"id"`
    Service        string      `json:"service"`
    Time           int64       `json:"time"`
    Version        string      `json:"version"`
    LambdaFunction string      `json:"lambdaFunction"`
    EventMsgs      []byte      `json:"eventMsgs"`
    Errors         []*DLQError `json:"errors"`
}
