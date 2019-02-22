package dynamostream

import (
	"fmt"
	"strconv"
	"testing"
)

func TestGroupConcurency(t *testing.T) {

	handler := func(msgs EventMsgs) error {
		for _, msg := range msgs {
			fmt.Println(msg.EventID, msg.EventType)
		}
		return nil
	}

	onErr := func(msgs EventMsgs, err error) {}

	n := 10
	cm := NewGroupConcurrency()
	cm.RegisterHandler(handler)
	cm.FilterEvents("et1", "et3")
	cm.ErrorHandlers(onErr)

	records := make(Records, n)
	for i := range records {
		rec := &Record{}
		istr := strconv.Itoa(i)
		if i == 0 || i == 4 || i == 7 {
			rec.add("p1", istr, "et1")
		}
		if i == 1 || i == 5 || i == 6 || i == 9 {
			rec.add("p2", istr, "et2")
		}
		if i == 2 || i == 3 || i == 8 {
			rec.add("p3", istr, "et3")
		}
		records[i] = rec
	}

	cm.Process(records)

	cm.Wait()
}
