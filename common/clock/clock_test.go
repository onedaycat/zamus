package clock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClock(t *testing.T) {
	tt := Now()
	require.False(t, tt.IsZero())

	now := time.Now().Add(time.Second * -10)
	Freeze(now)

	tt = Now()
	require.Equal(t, tt, now)

	UnFreeze()
	tt = Now()
	require.NotEqual(t, tt, now)
}
