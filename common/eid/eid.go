package eid

import (
	"strconv"

	"github.com/rs/xid"
)

var (
	emptyStr  = ""
	freezeaid = ""
)

func FreezeID(id string) {
	freezeaid = id
}

func UnFreezeID() {
	freezeaid = emptyStr
}

func CreateEventID(aggID string, seq int64) string {
	return aggID + strconv.FormatInt(seq, 10)
}

func GenerateID() string {
	if freezeaid != emptyStr {
		return freezeaid
	}

	return xid.New().String()
}
