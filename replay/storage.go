package replay

import (
    "github.com/onedaycat/zamus/event"
)

type Storage interface {
    Query(fromTime, toTime int64, eventTypes []string, aggID string) (event.Msgs, error)
}
