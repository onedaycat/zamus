package replay

import (
	"github.com/onedaycat/zamus/eventstore"
)

type Storage interface {
	Query(fromTime, toTime int64, eventTypes []string, aggID string) ([]*eventstore.EventMsg, error)
}
