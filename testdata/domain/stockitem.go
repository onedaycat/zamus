package domain

import (
	"github.com/onedaycat/zamus/common/clock"
	"github.com/onedaycat/zamus/eventstore"
)

type AccountProfile struct {
	*eventstore.AggregateBase
	AccountID string
	Fullname  string
	Country   string
}

func (a *AccountProfile) GetPartitionKey() string {
	return a.AccountID
}

func (a *AccountProfile) GetAggregateType() string {
	return "SellConnect.Account.AccountProfile"
}

type StockItem struct {
	*eventstore.AggregateBase
	ProductID string
	Qty       Qty
	RemovedAt int64
}

func NewStockItem() *StockItem {
	return &StockItem{
		AggregateBase: eventstore.InitAggregate(),
	}
}

func (st *StockItem) GetPartitionKey() string {
	return st.ProductID
}

func (st *StockItem) GetAggregateType() string {
	return "domain.subdomain.aggregate"
}

func (st *StockItem) Create(id, productID string, qty Qty) {
	st.SetAggregateID(id)
	st.ProductID = productID
	st.Qty = qty
	st.Publish(StockItemCreatedEvent, &StockItemCreated{
		ProductID: productID,
		Qty:       st.Qty.ToInt(),
	})
}

func (st *StockItem) Apply(msg *eventstore.EventMsg) error {
	switch msg.EventType {
	case StockItemCreatedEvent:
		event := &StockItemCreated{}
		if err := msg.UnmarshalPayload(event); err != nil {
			return err
		}

		st.ProductID = event.ProductID
		st.Qty = Qty(event.Qty)
	case StockItemUpdatedEvent:
		event := &StockItemUpdated{}
		if err := msg.UnmarshalPayload(event); err != nil {
			return err
		}

		st.ProductID = event.ProductID
		st.Qty = Qty(event.Qty)
	case StockItemRemovedEvent:
		event := &StockItemRemoved{}
		if err := msg.UnmarshalPayload(event); err != nil {
			return err
		}

		st.ProductID = event.ProductID
		st.RemovedAt = event.RemovedAt
		return nil
	}

	return nil
}

func (st *StockItem) Add(amount Qty) {
	st.Qty = st.Qty.Add(amount)
	st.Publish(StockItemUpdatedEvent, &StockItemUpdated{
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

	st.Publish(StockItemUpdatedEvent, &StockItemUpdated{
		ProductID: st.ProductID,
		Qty:       st.Qty.ToInt(),
	})

	return nil
}

func (st *StockItem) Remove() error {
	st.RemovedAt = clock.Now().Unix()

	st.Publish(StockItemRemovedEvent, &StockItemRemoved{
		ProductID: st.ProductID,
		RemovedAt: st.RemovedAt,
	})

	return nil
}

func (st *StockItem) IsRemoved() bool {
	return st.RemovedAt > 0
}
