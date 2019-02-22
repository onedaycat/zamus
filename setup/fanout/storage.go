package main

import (
	"github.com/onedaycat/zamus/eventstore"
)

type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg

type Storage interface {
	Save(consumers *Consumers) error
	Get() (*Consumers, error)
	Fanout(consumer *Consumer, msg EventMsgs) error
}
