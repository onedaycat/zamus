package domain

const StockItemRemovedEvent = "ecom:StockItemRemoved"

type XX_StockItemRemoved struct {
    ProductID string `json:"productID"`
    RemovedAt int64  `json:"removedAt"`
}
