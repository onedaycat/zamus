package dql

type DQLErrors = []*DQLError

type DQLError struct {
	Message string      `json:"message"`
	Cause   string      `json:"cause"`
	Input   interface{} `json:"input"`
	Stacks  []string    `json:"stacks"`
}

type DQLMsg struct {
	ID             string      `json:"id" db:"id"`
	Service        string      `json:"service" db:"service"`
	Time           int64       `json:"time" db:"time"`
	Version        string      `json:"version" db:"version"`
	LambdaFunction string      `json:"lambdaFunction" db:"lambdaFunction"`
	EventMsgs      []byte      `json:"eventMsgs" db:"eventMsgs"`
	Errors         []*DQLError `json:"errors" db:"errors"`
}
