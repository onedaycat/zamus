package kinesisstream

import (
	"fmt"
	"testing"
	"time"
)

func TestGroupConcurency(t *testing.T) {

	handler := func(msgs EventMsgs) (*EventMsg, error) {
		fmt.Println(len(msgs))
		for _, x := range msgs {
			fmt.Println(x.EventID)
			time.Sleep(time.Second)
		}
		return nil, nil
	}

	handler2 := func(msgs EventMsgs) (*EventMsg, error) {
		fmt.Println(len(msgs))
		for _, x := range msgs {
			fmt.Println(x.EventID)
		}
		time.Sleep(time.Second)
		return nil, nil
	}

	onErr := func(msg *EventMsg, err error) {}

	n := 10
	cm := NewGroupConcurrency()
	cm.RegisterEvent("et1", handler)
	cm.RegisterEvent("et3", handler2)
	cm.ErrorHandler(onErr)

	records := make(Records, n)
	for i := range records {
		rec := &Record{}
		if i == 0 || i == 4 || i == 7 {
			rec.add("1", "et1")
		}
		if i == 1 || i == 5 || i == 6 || i == 9 {
			rec.add("2", "et2")
		}
		if i == 2 || i == 3 || i == 8 {
			rec.add("3", "et3")
		}
		records[i] = rec
	}

	cm.Process(records)

	cm.Wait()
}
