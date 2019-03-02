package dql

import (
	"github.com/onedaycat/zamus/eventstore"
)

type Storage interface {
	Put(msg *eventstore.EventMsg) error
	GetList(msg *eventstore.EventMsg) error
}
