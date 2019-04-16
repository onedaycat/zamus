package clock

import "time"

var zeroTime = time.Time{}
var freeze time.Time

func Now() time.Time {
	if !freeze.IsZero() {
		return freeze
	}

	return time.Now().UTC()
}

func UnFreeze() {
	freeze = zeroTime
}

func Freeze(t time.Time) {
	freeze = t
}
