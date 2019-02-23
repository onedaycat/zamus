package dynamostream

import (
	"context"

	"github.com/onedaycat/zamus/eventstore"
)

type EventMsg = eventstore.EventMsg
type EventMsgs = []*eventstore.EventMsg
type EventMsgList = eventstore.EventMsgList

type LambdaHandler func(ctx context.Context, event *DynamoDBStreamEvent) (interface{}, error)
type EventMessageHandler func(msg *EventMsg) error
type EventMessagesHandler func(msgs EventMsgs) error
type EventMessageErrorHandler func(msg *EventMsg, err error)
type EventMessagesErrorHandler func(msgs EventMsgs, err error)
type KeyHandler func(record *Record) string
