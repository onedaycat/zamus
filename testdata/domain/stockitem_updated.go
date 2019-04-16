package domain

const StockItemUpdatedEvent = "domain.subdomain.aggregate.StockItemUpdated"

type XX_StockItemUpdated struct {
    ProductID string `json:"productID"`
    Qty       int    `json:"qty"`
}

type XX_StockItemUpdated2 struct {
    Qty int `json:"qty"`
}
