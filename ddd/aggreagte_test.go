package ddd_test

import (
	"testing"

	"github.com/onedaycat/zamus/testdata/domain"
	"github.com/stretchr/testify/require"
)

func TestAggregateIsNew(t *testing.T) {
	t.Run("HasIDAndSeq", func(t *testing.T) {
		st := domain.NewStockItem()
		st.SetAggregateID("1")
		st.SetSequence(10)

		require.False(t, st.IsNew())
	})

	t.Run("HasIDAndEvent", func(t *testing.T) {
		st := domain.NewStockItem()
		st.Create("1", "p1", 1)

		require.False(t, st.IsNew())
	})

	t.Run("HasIDAndNoSeq", func(t *testing.T) {
		st := domain.NewStockItem()
		st.SetAggregateID("1")

		require.True(t, st.IsNew())
	})

	t.Run("NoID", func(t *testing.T) {
		st := domain.NewStockItem()
		st.ProductID = "p1"

		require.True(t, st.IsNew())
	})
}
