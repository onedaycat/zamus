package gocqrs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewSeq(t *testing.T) {
	tt := time.Now().Unix()
	seq := NewSeq(tt, 10)
	require.Equal(t, (tt*100000)+10, seq)
}
