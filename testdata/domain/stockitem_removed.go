package domain

const StockItemRemovedEvent = "ecom:StockItemRemoved"

type StockItemRemoved struct {
	ProductID string `json:"productID"`
	RemovedAt int64  `json:"removedAt"`
}
