package dynamostream

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
)

func TestConcurency(t *testing.T) {

	getKey := func(record *Record) string {
		return *(record.DynamoDB.Keys["id"].S)
	}

	handler := func(msg *EventMessage) error {
		if msg.EventID == "eid3" {
			return errors.New("eid3")
		}
		fmt.Println("handle", msg.EventID)
		return nil
	}

	onErr := func(msg *EventMessage, err error) {
		fmt.Println("error:", err)
	}

	n := 10
	cm := newConcurrencyManager(n)

	records := make(Records, n)
	for i := range records {
		rec := &Record{}
		istr := strconv.Itoa(i)
		if i == 0 || i == 4 || i == 7 {
			rec.add("id", "1", "eid"+istr)
		}
		if i == 1 || i == 5 || i == 6 || i == 9 {
			rec.add("id", "2", "eid"+istr)
		}
		if i == 2 || i == 3 || i == 8 {
			rec.add("id", "3", "eid"+istr)
		}
		records[i] = rec
	}

	for _, record := range records {
		cm.Send(record, getKey, handler, onErr)
	}

	cm.Wait()
}
