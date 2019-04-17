package domain

import (
    "github.com/onedaycat/errors"
    "github.com/onedaycat/zamus/event"
    "github.com/onedaycat/zamus/eventstore"
    "github.com/onedaycat/zamus/internal/common/clock"
)

type CreateStockCmd struct {
    ProductID string
    Qty       int
}

type StockItem struct {
    *eventstore.AggregateBase
    ProductID string
    Qty       Qty
    RemovedAt int64
}

func NewStockItem() *StockItem {
    return &StockItem{
        AggregateBase: eventstore.InitAggregate(1),
    }
}

func (st *StockItem) Create(id, productID string, qty Qty) {
    st.SetAggregateID(id)
    st.ProductID = productID
    st.Qty = qty
    st.Publish(&StockItemCreated{
        Id:        id,
        ProductID: productID,
        Qty:       st.Qty.ToInt(),
    })
}

func (st *StockItem) Apply(msg *event.Msg) errors.Error {
    switch evt := msg.MustParseEvent().(type) {
    case *StockItemCreated:
        st.ProductID = evt.ProductID
        st.Qty = Qty(evt.Qty)
    case *StockItemUpdated:
        st.ProductID = evt.ProductID
        st.Qty = Qty(evt.Qty)
    case *StockItemRemoved:
        st.ProductID = evt.ProductID
        st.RemovedAt = evt.RemovedAt
    }

    return nil
}

func (st *StockItem) Add(amount Qty) {
    st.Qty = st.Qty.Add(amount)
    st.Publish(&StockItemUpdated{
        ProductID: st.ProductID,
        Qty:       st.Qty.ToInt(),
    })
}

func (st *StockItem) Sub(amount Qty) error {
    var err error
    st.Qty, err = st.Qty.Sub(amount)
    if err != nil {
        return err
    }

    st.Publish(&StockItemUpdated{
        ProductID: st.ProductID,
        Qty:       st.Qty.ToInt(),
    })

    return nil
}

func (st *StockItem) Remove() error {
    st.RemovedAt = clock.Now().Unix()

    st.Publish(&StockItemRemoved{
        ProductID: st.ProductID,
        RemovedAt: st.RemovedAt,
    })

    return nil
}

func (st *StockItem) IsRemoved() bool {
    return st.RemovedAt > 0
}
