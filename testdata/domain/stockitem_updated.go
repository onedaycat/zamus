package domain

const StockItemUpdatedEvent = "domain.subdomain.aggregate.StockItemUpdated"

type StockItemUpdated struct {
	ProductID string `json:"productID"`
	Qty       int    `json:"qty"`
}

type StockItemUpdated2 struct {
	Qty int `json:"qty"`
}
