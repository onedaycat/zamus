package domain

const StockItemCreatedEvent = "domain.subdomain.aggregate.StockItemCreated"

type StockItemCreated struct {
	ID        string `json:"id"`
	ProductID string `json:"productID"`
	Qty       int    `json:"qty"`
}
