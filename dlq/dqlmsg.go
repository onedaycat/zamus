package dlq

//noinspection GoNameStartsWithPackageName
type DLQErrors = []*DLQError

type LambdaType string

const (
    Reactor LambdaType = "reactor"
    Saga    LambdaType = "saga"
    Trigger LambdaType = "trigger"
)

//noinspection GoNameStartsWithPackageName
type DLQError struct {
    Message string      `json:"message"`
    Cause   string      `json:"cause"`
    Input   interface{} `json:"input"`
    Stacks  []string    `json:"stacks"`
}

//noinspection GoNameStartsWithPackageName
type DLQMsg struct {
    ID         string      `json:"id"`
    Service    string      `json:"service"`
    Time       int64       `json:"time"`
    Version    string      `json:"version"`
    Data       []byte      `json:"data"`
    Errors     []*DLQError `json:"errors"`
    Fn         string      `json:"fn"`
    LambdaType LambdaType  `json:"lambdaType"`
}
