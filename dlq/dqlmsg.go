package dlq

//noinspection GoNameStartsWithPackageName
type DLQErrors = []*DLQError

type SourceType string

const (
    SQS            SourceType = "sqs"
    SNS            SourceType = "sns"
    Lambda         SourceType = "lambda"
    Kinesis        SourceType = "kinesis"
    DynamoDBStream SourceType = "dynamodb"
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
    EventMsgs  []byte      `json:"eventMsgs"`
    Errors     []*DLQError `json:"errors"`
    SourceType SourceType  `json:"sourceType"`
    Source     string      `json:"source"`
}
