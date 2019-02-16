package eid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateEID(t *testing.T) {
	id := CreateEventID("bh9vq3krtr34lt1oedc0", 99)
	require.Len(t, id, 22)
}

func TestGenerateAggregateID(t *testing.T) {
	FreezeID("1")
	id := GenerateID()
	require.Equal(t, "1", id)

	UnFreezeID()
	id = GenerateID()
	require.Len(t, id, 20)
}
