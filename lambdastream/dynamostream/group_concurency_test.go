package dynamostream

import (
	"fmt"
	"strconv"
	"testing"
)

func TestGroupConcurency(t *testing.T) {

	getKey := func(record *Record) string {
		return *(record.DynamoDB.Keys["id"].S)
	}

	handler := func(msgs EventMessages) error {
		fmt.Println(len(msgs))
		for _, x := range msgs {
			fmt.Println(x.EventID)
		}
		return nil
	}

	onErr := func(msgs EventMessages, err error) {

	}

	n := 10
	cm := newGroupConcurrencyManager(n)

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

	cm.Send(records, getKey, handler, onErr)

	cm.Wait()
}
