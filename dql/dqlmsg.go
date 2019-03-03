package dql

type DQLMsgs = []*DQLMsg

type DQLMsg struct {
	ID             string   `json:"id" db:"id"`
	Service        string   `json:"service" db:"service"`
	Version        string   `json:"version" db:"version"`
	LambdaFunction string   `json:"lambdaFunction" db:"lambdaFunction"`
	EventType      string   `json:"eventType" db:"eventType"`
	AggregateID    string   `json:"aggregateID" db:"aggregateID"`
	EventID        string   `json:"eventID" db:"eventID"`
	Seq            int64    `json:"seq" db:"seq"`
	Time           int64    `json:"time" db:"time"`
	DQLTime        int64    `json:"dqlTime" db:"dqlTime"`
	EventMsg       []byte   `json:"eventMsg" db:"eventMsg"`
	Error          []string `json:"error" db:"error"`
}
